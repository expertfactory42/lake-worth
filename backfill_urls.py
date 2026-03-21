"""
Backfill image URLs for existing processed_pdfs entries.

Scrapes search result pages to collect image URLs and matches them
to existing DB entries using metadata visible on the search results page.
Does NOT visit individual image pages, so no throttle needed.

Usage:
    python backfill_urls.py
"""

import os
import re
import time
import sqlite3
import logging
from datetime import datetime, timedelta

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

# === LOGGING ===
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"backfill_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("backfill")

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_missing_count(conn):
    return conn.execute(
        "SELECT COUNT(*) FROM processed_pdfs WHERE (url IS NULL OR url = '') AND search_term = ?",
        (SEARCH_TERM,)
    ).fetchone()[0]


def get_missing_filenames(conn):
    """Get set of pdf_filenames that need URLs."""
    rows = conn.execute(
        "SELECT pdf_filename FROM processed_pdfs WHERE (url IS NULL OR url = '') AND search_term = ?",
        (SEARCH_TERM,)
    ).fetchall()
    return set(r[0] for r in rows)


def get_earliest_missing_date(conn):
    rows = conn.execute(
        "SELECT pdf_filename FROM processed_pdfs WHERE (url IS NULL OR url = '') AND search_term = ?",
        (SEARCH_TERM,)
    ).fetchall()
    earliest = None
    for row in rows:
        m = re.search(r'(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', row[0])
        if m:
            d = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            if not earliest or d < earliest:
                earliest = d
    return earliest or "1909-01-01"


def setup_driver():
    options = uc.ChromeOptions()
    temp_profile = r"c:\lake_worth\chrome_temp_profile"
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=145)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    return driver


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
                            time.sleep(4)
                            return True
                except StaleElementReferenceException:
                    continue
    except Exception:
        pass
    return False


def extract_results_with_metadata(driver):
    """Extract image URLs and their surrounding text from search results.

    Returns list of (url, text) tuples where text is the visible text
    near the link that may contain date/newspaper/page info.
    """
    results = []
    try:
        # Get all result containers - try various selectors
        entries = driver.execute_script("""
            var results = [];
            // Try to find result items by their link
            var links = document.querySelectorAll('a[href*="/image/"]');
            var seen = {};
            for (var i = 0; i < links.length; i++) {
                var a = links[i];
                var href = a.href;
                if (!href || seen[href]) continue;
                seen[href] = true;
                // Get text from the link and its parent container
                var container = a.closest('li, article, div[class*="result"], div[class*="item"]') || a.parentElement;
                var text = container ? container.innerText : a.innerText;
                results.push({url: href, text: text});
            }
            return results;
        """)
        for entry in entries:
            if entry.get("url") and "/image/" in entry["url"]:
                results.append((entry["url"], entry.get("text", "")))
    except Exception as e:
        log.warning(f"  Error extracting results: {e}")
    return results


def text_to_pdf_filename(text, url):
    """Try to parse newspaper, date, page from search result text to build pdf_filename."""
    # Extract newspaper name
    newspaper = "Fort_Worth_Star_Telegram"
    if re.search(r'Record[- ]?Telegram', text, re.IGNORECASE):
        newspaper = "Fort_Worth_Record_Telegram"
    elif re.search(r'Fort Worth Record', text, re.IGNORECASE):
        newspaper = "Fort_Worth_Record"
    elif re.search(r'Star[- ]?Telegram', text, re.IGNORECASE):
        newspaper = "Fort_Worth_Star_Telegram"

    # Extract date - try various formats
    date_str = ""
    # "Mon DD, YYYY" or "Month DD, YYYY"
    dm = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', text)
    if dm:
        month_str = dm.group(1)[:3].lower()
        month = MONTH_MAP.get(month_str, "")
        if month:
            day = dm.group(2).zfill(2)
            year = dm.group(3)
            date_str = f"{year}_{month}_{day}"

    # Extract page number
    page = 0
    pm = re.search(r'[Pp]age\s*(\d+)', text)
    if pm:
        page = int(pm.group(1))

    if date_str and page:
        return f"{newspaper}_{date_str}_{page}.pdf"
    return None


def main(max_updates=0):
    """Run backfill. max_updates=0 means unlimited."""
    conn = get_db()
    missing = get_missing_count(conn)
    missing_set = get_missing_filenames(conn)
    start_date = get_earliest_missing_date(conn)

    log.info("=" * 60)
    log.info("URL Backfill (no page visits)")
    log.info("=" * 60)
    log.info(f"  Entries missing URLs: {missing}")
    log.info(f"  Starting from: {start_date}")
    log.info(f"  Max updates: {max_updates or 'unlimited'}")
    log.info(f"  Log: {log_filename}")

    updated = 0
    unmatched = 0
    session = 0
    MAX_SHOW_MORE = 5  # limit clicks per session to keep batches small

    try:
        current_date = start_date
        driver = None

        while current_date <= DATE_END and len(missing_set) > 0:
            if max_updates and updated >= max_updates:
                log.info(f"  Reached max_updates limit ({max_updates})")
                break

            session += 1

            if driver is None or session % 30 == 0:
                if driver:
                    try:
                        driver.quit()
                    except Exception:
                        pass
                driver = setup_driver()
                driver.get("https://star-telegram.newspapers.com/")
                time.sleep(5)

            # Use a 90-day window so we don't try to load the entire date range
            window_end_dt = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=90)
            window_end = min(window_end_dt.strftime("%Y-%m-%d"), DATE_END)

            encoded_term = SEARCH_TERM.replace(" ", "+")
            search_url = (
                "https://star-telegram.newspapers.com/search/results/"
                f"?date-end={window_end}&date-start={current_date}"
                f"&keyword=%22{encoded_term}%22"
                "&sort=paper-date-asc"
            )

            log.info(f"\n>>> SESSION {session}: {current_date} to {window_end}")
            driver.get(search_url)
            time.sleep(4)

            # Collect results with limited "Show More" clicks
            all_results = []
            prev_count = 0
            show_more_clicks = 0
            while True:
                results = extract_results_with_metadata(driver)
                seen_urls = set(r[0] for r in all_results)
                new_results = [(u, t) for u, t in results if u not in seen_urls]
                if new_results:
                    all_results.extend(new_results)
                    log.info(f"  {len(all_results)} results collected...")

                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)

                if len(all_results) == prev_count:
                    if show_more_clicks >= MAX_SHOW_MORE:
                        break
                    if click_show_more(driver):
                        show_more_clicks += 1
                    else:
                        break
                prev_count = len(all_results)

            log.info(f"  Total results this batch: {len(all_results)}")

            latest_date = current_date
            session_updated = 0

            for url, text in all_results:
                if max_updates and updated >= max_updates:
                    break

                pdf_filename = text_to_pdf_filename(text, url)

                if pdf_filename and pdf_filename in missing_set:
                    conn.execute(
                        "UPDATE processed_pdfs SET url = ? WHERE pdf_filename = ? AND (url IS NULL OR url = '')",
                        (url, pdf_filename)
                    )
                    conn.commit()
                    missing_set.discard(pdf_filename)
                    updated += 1
                    session_updated += 1
                    log.info(f"    MATCHED: {pdf_filename}")

                    # Track latest date
                    dm = re.search(r'(\d{4})_(\d{2})_(\d{2})', pdf_filename)
                    if dm:
                        d = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                        if d > latest_date:
                            latest_date = d
                elif pdf_filename:
                    # Track date even for already-filled entries
                    dm = re.search(r'(\d{4})_(\d{2})_(\d{2})', pdf_filename)
                    if dm:
                        d = f"{dm.group(1)}-{dm.group(2)}-{dm.group(3)}"
                        if d > latest_date:
                            latest_date = d
                else:
                    unmatched += 1

            log.info(f"  Session {session}: {session_updated} updated (total: {updated}, remaining: {len(missing_set)})")

            # Advance date past the window we just processed
            if latest_date > current_date:
                # Jump to day after latest match to avoid re-processing
                dt = datetime.strptime(latest_date, "%Y-%m-%d") + timedelta(days=1)
                current_date = dt.strftime("%Y-%m-%d")
            else:
                # No results matched at all, skip past the window
                dt = datetime.strptime(window_end, "%Y-%m-%d") + timedelta(days=1)
                current_date = dt.strftime("%Y-%m-%d")

            # Pause between sessions to avoid search throttle
            time.sleep(10)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    except Exception as e:
        log.error(f"Error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    remaining = get_missing_count(conn)
    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  URLs updated: {updated}")
    log.info(f"  Unmatched results: {unmatched}")
    log.info(f"  Still missing: {remaining}")
    log.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    import sys
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    main(max_updates=limit)
