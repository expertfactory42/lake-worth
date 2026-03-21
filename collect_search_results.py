"""
Collect search result metadata from newspapers.com without downloading PDFs.

Opens Chrome, runs the search, visits each result page briefly to grab
the title (date, newspaper, page), and creates entries in the database
so they appear in the dashboard "No Articles" tab with WWW buttons.

User then manually clips articles and drags clip URLs into the dashboard.

Usage:
    python collect_search_results.py
"""

import os
import re
import time
import sqlite3
import logging
from pathlib import Path
from datetime import datetime

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException
)

# === CONFIGURATION ===
DB_PATH = r"c:\lake_worth\lake_worth.db"
LOG_DIR = r"c:\lake_worth\collector_logs"
DATE_END = "1925-12-31"
SEARCH_TERM = "lake worth"
MAX_NEW_ENTRIES = 0  # Stop after this many new entries (0 = unlimited)
WAIT_TIMEOUT = 15
ACTION_DELAY = 2

# === LOGGING SETUP ===
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"collector_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("collector")

# Separate file for failed/skipped URLs so they can be retried
errors_filename = os.path.join(LOG_DIR, f"errors_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
error_log = logging.getLogger("collector.errors")
error_handler = logging.FileHandler(errors_filename)
error_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))
error_log.addHandler(error_handler)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def get_start_date(conn, search_term, advance_past=False):
    """Find the latest date across both articles and processed_pdfs to resume from.

    If advance_past=True, add one day to skip past dates we've already fully covered.
    """
    latest = None

    # Check processed_pdfs
    for row in conn.execute(
        "SELECT pdf_filename FROM processed_pdfs WHERE search_term = ?", (search_term,)
    ).fetchall():
        m = re.search(r'(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', row[0])
        if m:
            d = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            if not latest or d > latest:
                latest = d

    # Check articles
    row = conn.execute(
        "SELECT MAX(date) FROM articles WHERE search_term = ?", (search_term,)
    ).fetchone()
    if row and row[0] and (not latest or row[0] > latest):
        latest = row[0]

    if latest:
        if advance_past:
            from datetime import timedelta
            dt = datetime.strptime(latest, "%Y-%m-%d") + timedelta(days=1)
            return dt.strftime("%Y-%m-%d")
        return latest

    # Default start dates by search term
    if search_term == "lake worth":
        return "1913-07-01"
    return "1909-01-01"


def build_search_url(start_date, search_term):
    encoded_term = search_term.replace(" ", "+")
    return (
        "https://star-telegram.newspapers.com/search/results/"
        f"?date-end={DATE_END}&date-start={start_date}"
        f"&keyword=%22{encoded_term}%22"
        "&sort=paper-date-asc"
    )


def setup_driver():
    options = uc.ChromeOptions()
    temp_profile = r"c:\lake_worth\chrome_temp_profile"
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    return driver


def collect_search_results(driver):
    """Collect result links from the current search results page."""
    results = []
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/image/']"))
        )
        time.sleep(ACTION_DELAY)
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/image/']")
        seen = set()
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and href not in seen and "/image/" in href:
                    seen.add(href)
                    results.append(href)
            except StaleElementReferenceException:
                continue
    except TimeoutException:
        print("  No results found.")
    return results


def click_show_more(driver):
    try:
        for tag in ["button", "a", "[role='button']"]:
            elements = driver.find_elements(By.CSS_SELECTOR, tag)
            for el in elements:
                try:
                    text = el.text.strip().lower()
                    if "show more" in text or "load more" in text:
                        if el.is_displayed() and el.is_enabled():
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block: 'center'});", el)
                            time.sleep(1)
                            el.click()
                            print("    Clicked 'Show More Results'")
                            time.sleep(ACTION_DELAY * 2)
                            return True
                except StaleElementReferenceException:
                    continue
    except Exception:
        pass
    return False


def verify_phrase_match(driver, search_term):
    """Check if the page actually contains the exact search phrase.

    newspapers.com highlights matched words on the image page. We grab all
    visible text and check for the exact phrase. If the page only has the
    words separately (e.g. "lake ... worth"), return False.
    """
    try:
        # Method 1: Check the search bar match indicator text
        # The top bar shows something like '"lake wo" 1 of 2 matches'
        # But more reliably, check the page body text
        body_text = driver.execute_script(
            "return document.body ? document.body.innerText : '';"
        ).lower()

        # Check for exact phrase in any visible text on the page
        term = search_term.lower()
        if term in body_text:
            return True

        # Method 2: Check highlighted/marked elements for adjacency
        highlights = driver.execute_script("""
            var marks = document.querySelectorAll('mark, [class*="highlight"], [class*="match"]');
            var texts = [];
            marks.forEach(function(m) { texts.push(m.textContent.trim().toLowerCase()); });
            return texts;
        """)
        if highlights:
            # Check if consecutive highlights form the phrase
            combined = " ".join(highlights)
            if term in combined:
                return True

        # Method 3: Check the page title area which sometimes has snippet text
        # If none of the above found it, it's likely a false positive
        return False
    except Exception:
        # If we can't verify, let it through rather than skip a real match
        return True


MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}


def parse_page_title(title, url):
    """Parse the image page title into metadata."""
    # Extract newspaper name
    newspaper = "Fort Worth Star Telegram"
    m = re.search(
        r'(Fort Worth (?:Star-Telegram|Record-Telegram|Record Telegram|Star Telegram|Record))',
        title, re.IGNORECASE
    )
    if m:
        newspaper = m.group(1)

    # Extract date
    date_match = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', title)
    date_str = ""
    if date_match:
        month_str = date_match.group(1)[:3].lower()
        month = MONTH_MAP.get(month_str, "00")
        day = date_match.group(2).zfill(2)
        year = date_match.group(3)
        date_str = f"{year}-{month}-{day}"

    # Extract page number
    page_match = re.search(r'page\s*(\d+)', title, re.IGNORECASE)
    page = int(page_match.group(1)) if page_match else 0

    # Build a synthetic PDF filename for consistency
    paper_clean = re.sub(r'[^a-zA-Z0-9]+', '_', newspaper).strip('_')
    if date_str and page:
        pdf_filename = f"{paper_clean}_{date_str.replace('-', '_')}_{page}.pdf"
    else:
        img_match = re.search(r'/image/(\d+)', url)
        img_id = img_match.group(1) if img_match else str(int(time.time()))
        pdf_filename = f"{paper_clean}_{img_id}.pdf"

    return {
        "newspaper": newspaper,
        "date": date_str,
        "page": page,
        "pdf_filename": pdf_filename,
        "url": url,
    }


def entry_exists(conn, pdf_filename):
    row = conn.execute(
        "SELECT 1 FROM processed_pdfs WHERE pdf_filename = ?", (pdf_filename,)
    ).fetchone()
    return row is not None


def update_url(conn, pdf_filename, url):
    """Backfill the URL for an existing entry if missing."""
    if url:
        conn.execute(
            "UPDATE processed_pdfs SET url = ? WHERE pdf_filename = ? AND (url IS NULL OR url = '')",
            (url, pdf_filename),
        )
        conn.commit()


def create_entry(conn, meta, search_term):
    """Create a processed_pdfs entry with no PDF file — just metadata for the dashboard."""
    conn.execute(
        "INSERT OR IGNORE INTO processed_pdfs (pdf_filename, articles_found, search_term, url) VALUES (?, 0, ?, ?)",
        (meta["pdf_filename"], search_term, meta.get("url", "")),
    )
    conn.commit()


RESTART_EVERY = 100  # Close browser and restart from latest date every N new entries
MAX_CONSECUTIVE_EXISTS = 10
VIEWS_PER_HOUR = 500  # Throttle: max page views per rolling hour

# Rolling window of page-view timestamps for rate limiting
_view_times = []


def track_view():
    """Record a page view and pause if we've hit the hourly limit."""
    now = time.time()
    _view_times.append(now)

    # Prune views older than 1 hour
    cutoff = now - 3600
    while _view_times and _view_times[0] < cutoff:
        _view_times.pop(0)

    if len(_view_times) >= VIEWS_PER_HOUR:
        oldest = _view_times[0]
        wait_until = oldest + 3600
        wait_secs = wait_until - now
        if wait_secs > 0:
            log.info(f"  >>> THROTTLE: {len(_view_times)} views in the last hour. Pausing {int(wait_secs)}s until {datetime.fromtimestamp(wait_until).strftime('%H:%M:%S')}...")
            time.sleep(wait_secs)
            # Prune again after sleeping
            now2 = time.time()
            cutoff2 = now2 - 3600
            while _view_times and _view_times[0] < cutoff2:
                _view_times.pop(0)


def run_session(conn, search_term, advance_past=False):
    """Run one browser session. Returns (new_entries, skipped, finished).

    finished=True means no more results to find; False means we hit
    the restart threshold and should start a new session.
    """
    start_date = get_start_date(conn, search_term, advance_past=advance_past)
    search_url = build_search_url(start_date, search_term)

    log.info("=" * 60)
    log.info(f"  Starting session from: {start_date}")
    log.info(f"  Search URL: {search_url}")
    log.info("=" * 60)

    driver = None
    seen_urls = set()
    new_entries = 0
    skipped = 0
    consecutive_exists = 0
    finished = False

    try:
        driver = setup_driver()

        log.info("Opening Newspapers.com...")
        driver.get("https://star-telegram.newspapers.com/")
        time.sleep(5)

        log.info("Loading search results...")
        driver.get(search_url)
        time.sleep(ACTION_DELAY * 2)

        batch = 0
        while True:
            batch += 1
            log.info(f"--- Batch {batch} ---")

            results = collect_search_results(driver)
            new_results = [u for u in results if u not in seen_urls]

            if not new_results:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(ACTION_DELAY)
                if click_show_more(driver):
                    continue
                else:
                    log.info("No more results.")
                    finished = True
                    break

            log.info(f"  Found {len(new_results)} new result links")

            for i, url in enumerate(new_results, 1):
                seen_urls.add(url)

                try:
                    track_view()
                    driver.get(url)
                    time.sleep(3)
                    title = driver.title or ""
                except Exception as e:
                    log.warning(f"  [{i}] Page load error: {e}")
                    error_log.error(f"PAGE_LOAD_FAIL | {url} | {e}")
                    continue

                meta = parse_page_title(title, url)

                if not meta["date"]:
                    log.warning(f"  [{i}] Could not parse: {title[:60]}")
                    error_log.error(f"PARSE_FAIL | {url} | title: {title[:80]}")
                    continue

                if search_term == "lake worth" and meta["date"] < "1913-01-01":
                    skipped += 1
                    log.info(f"  [{i}] SKIP (pre-1913): {meta['pdf_filename']}")
                    continue

                if entry_exists(conn, meta["pdf_filename"]):
                    update_url(conn, meta["pdf_filename"], meta.get("url", ""))
                    skipped += 1
                    consecutive_exists += 1
                    log.info(f"  [{i}] Exists: {meta['pdf_filename']}")
                    if consecutive_exists >= MAX_CONSECUTIVE_EXISTS:
                        log.info(f"  {MAX_CONSECUTIVE_EXISTS} consecutive existing entries. Skipping ahead.")
                        break
                    continue

                consecutive_exists = 0
                create_entry(conn, meta, search_term)
                new_entries += 1
                log.info(f"  [{i}] NEW: {meta['pdf_filename']} — {meta['newspaper']}, {meta['date']}, p{meta['page']}")

                if MAX_NEW_ENTRIES and new_entries >= MAX_NEW_ENTRIES:
                    log.info(f"  Reached {MAX_NEW_ENTRIES} new entries. Stopping.")
                    return new_entries, skipped, True

                if new_entries % RESTART_EVERY == 0:
                    log.info(f"  {RESTART_EVERY} new entries — restarting browser from latest date...")
                    return new_entries, skipped, False

            # Return to search for more
            log.info("  Returning to search...")
            driver.get(search_url)
            time.sleep(ACTION_DELAY * 2)

            # Load more results past what we've seen
            unseen = []
            while True:
                current = collect_search_results(driver)
                unseen = [u for u in current if u not in seen_urls]
                if unseen:
                    break
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(ACTION_DELAY)
                if not click_show_more(driver):
                    break
                time.sleep(ACTION_DELAY * 2)

            if not unseen:
                if new_entries > 0:
                    log.info("No more results on this page — restarting with updated date...")
                    break
                else:
                    log.info("No more results to load.")
                    finished = True
                    break

    except Exception as e:
        log.error(f"Session error: {e}")
        error_log.error(f"SESSION_CRASH | {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    return new_entries, skipped, finished


def main():
    conn = get_db()

    log.info("=" * 60)
    log.info("Newspapers.com Metadata Collector")
    log.info("=" * 60)
    log.info(f"  Search term: \"{SEARCH_TERM}\"")
    log.info(f"  Restart every: {RESTART_EVERY} new entries")
    log.info(f"  Throttle: {VIEWS_PER_HOUR} views/hour")
    log.info(f"  Log file: {log_filename}")
    log.info(f"  Error log: {errors_filename}")

    total_new = 0
    total_skipped = 0
    session = 0

    consecutive_failures = 0
    advance_past = False
    MAX_FAILURES = 3

    try:
        while True:
            session += 1
            log.info(f"\n>>> SESSION {session}")

            try:
                new, skipped, finished = run_session(conn, SEARCH_TERM, advance_past=advance_past)
            except Exception as e:
                log.error(f"  Session {session} crashed: {e}")
                error_log.error(f"SESSION_CRASH | session {session} | {e}")
                consecutive_failures += 1
                if consecutive_failures >= MAX_FAILURES:
                    log.error(f"  {MAX_FAILURES} consecutive failures. Giving up.")
                    break
                wait = 10 * consecutive_failures
                log.info(f"  Retrying in {wait} seconds...")
                time.sleep(wait)
                continue

            total_new += new
            total_skipped += skipped

            log.info(f"  Session {session}: {new} new, {skipped} skipped (running total: {total_new} new)")

            if new > 0:
                consecutive_failures = 0
                advance_past = False
            else:
                consecutive_failures += 1
                advance_past = True  # Skip past the current date next session
                if consecutive_failures >= MAX_FAILURES:
                    # Before giving up, try advancing one more day
                    next_date = get_start_date(conn, SEARCH_TERM, advance_past=True)
                    if next_date <= DATE_END:
                        log.info(f"  {consecutive_failures} sessions with no new entries. Advancing to {next_date}...")
                        consecutive_failures = 0
                    else:
                        log.warning(f"  {MAX_FAILURES} sessions with no new entries and past end date. Stopping.")
                        break

            if finished:
                if new == 0 and skipped > 0:
                    # Search returned only existing entries — advance past this date
                    next_date = get_start_date(conn, SEARCH_TERM, advance_past=True)
                    if next_date <= DATE_END:
                        log.info(f"  No new results but found existing entries. Advancing to {next_date}...")
                        advance_past = True
                        consecutive_failures = 0
                        continue
                log.info("  All results collected.")
                break

            log.info("  Restarting in 3 seconds...")
            time.sleep(3)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Sessions: {session}")
    log.info(f"  Total new entries: {total_new}")
    log.info(f"  Total skipped: {total_skipped}")
    log.info(f"  Error log: {errors_filename}")
    log.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
