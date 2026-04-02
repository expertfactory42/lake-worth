"""
Fix missing page URLs in processed_pdfs table.

For each entry missing a URL, constructs the same search URL the dashboard
uses (single-date search), visits it, collects all /image/ URLs from results,
matches them to DB entries by pdf_filename, and updates the url field.
Also downloads the search result thumbnail image for each entry.

Groups by (date, search_term) so each search page is visited only once,
fixing all entries for that date in one go.

Usage:
    python fix_missing_urls.py [max_searches]
"""

import os
import re
import sys
import time
import json
import base64
import sqlite3
import logging
import requests
from datetime import datetime
from collections import defaultdict

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException,
)

sys.path.insert(0, os.path.dirname(__file__))
from clip_and_extract import DB_PATH, parse_page_title

# === LOGGING ===
LOG_DIR = r"c:\lake_worth\collector_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"fix_urls_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("fix_urls")

WAIT_TIMEOUT = 15
ACTION_DELAY = 2
THUMB_DIR = r"c:\lake_worth\thumbnails"
os.makedirs(THUMB_DIR, exist_ok=True)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_missing_url_entries(conn):
    """Group entries missing URLs by (date, search_term).
    Returns dict: (date_str, search_term) -> set of pdf_filenames
    """
    rows = conn.execute("""
        SELECT pdf_filename, search_term
        FROM processed_pdfs
        WHERE (url IS NULL OR url = '')
    """).fetchall()

    groups = defaultdict(set)
    for r in rows:
        fname = r["pdf_filename"]
        term = r["search_term"]
        m = re.match(r'^.+?_(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', fname)
        if m:
            date_str = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
            groups[(date_str, term)].add(fname)

    return groups


def setup_driver():
    temp_profile = r"c:\lake_worth\chrome_temp_profile_clipper"
    os.makedirs(temp_profile, exist_ok=True)
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=146)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)
    driver.set_script_timeout(60)

    # Check login
    driver.get("https://star-telegram.newspapers.com/")
    time.sleep(5)
    page_text = driver.execute_script("return document.body.innerText || '';").lower()
    if "sign in" in page_text or "log in" in page_text:
        log.info("NOT LOGGED IN — please log in now.")
        log.info("Waiting up to 60 seconds for login...")
        for i in range(60):
            time.sleep(1)
            try:
                page_text = driver.execute_script("return document.body.innerText || '';").lower()
                if "sign in" not in page_text and "log in" not in page_text:
                    log.info("Login detected!")
                    break
            except Exception:
                pass
        else:
            log.warning("Login timeout — proceeding anyway.")
    else:
        log.info("Already logged in.")

    return driver


def collect_search_results(driver):
    """Collect result links, metadata, and thumbnail URLs from search results page."""
    results = []
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/image/']"))
        )
        time.sleep(ACTION_DELAY)
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/image/']")
        url_data = {}
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and "/image/" in href:
                    text = link.text.strip()
                    if href not in url_data or len(text) > len(url_data[href]):
                        url_data[href] = text
            except StaleElementReferenceException:
                continue

        # Build thumbnail lookup: image_id -> thumbnail src
        thumb_lookup = {}
        imgs = driver.find_elements(By.CSS_SELECTOR, "img[src*='/img/thumbnail/']")
        for img in imgs:
            try:
                src = img.get_attribute("src") or ""
                m = re.search(r'/img/thumbnail/(\d+)/', src)
                if m:
                    thumb_lookup[m.group(1)] = src
            except StaleElementReferenceException:
                continue

        for href, text in url_data.items():
            meta = parse_page_title(text, href)
            # Extract image_id from URL to find matching thumbnail
            img_match = re.search(r'/image/(\d+)', href)
            img_id = img_match.group(1) if img_match else None
            thumb_url = thumb_lookup.get(img_id, "") if img_id else ""
            results.append({
                "url": href,
                "pdf_filename": meta["pdf_filename"],
                "thumb_url": thumb_url,
            })
    except TimeoutException:
        pass
    return results


def click_show_more(driver):
    """Click 'Show More' button if present. Returns True if clicked."""
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


def download_thumbnails_batch(driver, entries):
    """Download all thumbnail images via browser JS fetch. Returns dict: pdf_filename -> relative path.
    entries: list of {pdf_filename, thumb_url}
    Must be called while browser is still on the search results page.
    """
    results = {}
    to_fetch = [(e["pdf_filename"], e["thumb_url"]) for e in entries if e.get("thumb_url")]
    if not to_fetch:
        return results

    # Use JS to fetch all thumbnails as base64 in one batch
    url_list = [url for _, url in to_fetch]
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

    for i, (fname, thumb_url) in enumerate(to_fetch):
        b64_data = b64_list[i] if i < len(b64_list) else ""
        if b64_data:
            try:
                img_bytes = base64.b64decode(b64_data)
                if len(img_bytes) > 100:
                    base = fname.replace(".pdf", "")
                    out_fname = f"{base}.jpg"
                    fpath = os.path.join(THUMB_DIR, out_fname)
                    with open(fpath, "wb") as f:
                        f.write(img_bytes)
                    results[fname] = f"thumbnails/{out_fname}"
            except Exception as e:
                log.warning(f"    Thumbnail save failed for {fname}: {e}")

    return results


def check_and_handle_502(driver):
    """Check for 502 Bad Gateway and refresh if found. Returns True if 502 was detected."""
    try:
        page_text = driver.execute_script("return document.body.innerText || '';").lower()
        title = driver.title.lower()
        if "502" in page_text or "bad gateway" in page_text or "502" in title:
            log.warning("    502 Bad Gateway detected — refreshing...")
            driver.refresh()
            time.sleep(6)
            return True
    except Exception:
        pass
    return False


def search_date(driver, date_str, search_term, needed_filenames=None):
    """Search for all results on a specific date. Returns list of {url, pdf_filename}.
    If needed_filenames is provided, stops clicking Show More once all are found.
    """
    encoded_term = search_term.replace(" ", "+")
    search_url = (
        "https://star-telegram.newspapers.com/search/results/"
        f"?date-end={date_str}&date-start={date_str}"
        f"&keyword=%22{encoded_term}%22"
        "&sort=paper-date-asc"
    )

    driver.get(search_url)
    time.sleep(4)

    # Handle 502 with up to 3 retries
    for retry in range(3):
        if check_and_handle_502(driver):
            continue
        break
    else:
        log.warning(f"    Persistent 502 for {date_str} — skipping")
        return []

    all_results = collect_search_results(driver)
    if not all_results:
        # One more 502 check on empty results
        if check_and_handle_502(driver):
            time.sleep(4)
            all_results = collect_search_results(driver)
        if not all_results:
            return []

    # Check if we already have all needed entries
    def all_found(results):
        if not needed_filenames:
            return False
        found = {r["pdf_filename"] for r in results}
        return needed_filenames.issubset(found)

    if all_found(all_results):
        return all_results

    # Click "Show More" until all needed results found or no more pages
    prev_count = 0
    stale_rounds = 0
    while True:
        current_count = len(all_results)
        if current_count == prev_count:
            stale_rounds += 1
            if stale_rounds >= 2:
                break
        else:
            stale_rounds = 0
        prev_count = current_count

        if not click_show_more(driver):
            break

        all_results = collect_search_results(driver)

        if all_found(all_results):
            break

    return all_results


def main(max_searches=0):
    conn = get_db()
    groups = get_missing_url_entries(conn)

    # Sort by date
    sorted_keys = sorted(groups.keys())

    total_entries = sum(len(v) for v in groups.values())

    log.info("=" * 60)
    log.info("URL Fixer — Fill missing page URLs from search results")
    log.info("=" * 60)
    log.info(f"  Unique date/term searches: {len(sorted_keys)}")
    log.info(f"  Total entries missing URLs: {total_entries}")
    log.info(f"  Max searches: {max_searches or 'unlimited'}")
    log.info(f"  Log: {log_filename}")

    if not sorted_keys:
        log.info("Nothing to fix.")
        return

    driver = setup_driver()

    searches_done = 0
    urls_fixed = 0
    not_matched = 0
    keep_browser_open = False

    try:
        for date_str, term in sorted_keys:
            if max_searches and searches_done >= max_searches:
                log.info(f"Reached max_searches limit ({max_searches})")
                break

            needed = groups[(date_str, term)]
            log.info(f"  [{searches_done + 1}/{len(sorted_keys)}] {date_str} \"{term}\" — {len(needed)} entries")

            try:
                results = search_date(driver, date_str, term, needed_filenames=needed)
                log.info(f"    Search returned {len(results)} results")

                # Check for Cloudflare challenge on empty results
                if not results:
                    page_text = driver.execute_script("return document.body.innerText || '';").lower()
                    if "challenge" in page_text or "verify" in page_text:
                        log.warning("Possible Cloudflare challenge. Stopping. Browser left open.")
                        keep_browser_open = True
                        break

                # Build lookup: pdf_filename -> {url, thumb_url}
                url_lookup = {}
                for r in results:
                    url_lookup[r["pdf_filename"]] = {
                        "url": r["url"],
                        "thumb_url": r.get("thumb_url", ""),
                    }

                # Download thumbnails in batch (while still on search page)
                to_download = [
                    {"pdf_filename": fname, "thumb_url": url_lookup[fname]["thumb_url"]}
                    for fname in needed if fname in url_lookup
                ]
                thumb_paths = download_thumbnails_batch(driver, to_download)

                # Match and update
                matched = 0
                for fname in needed:
                    if fname in url_lookup:
                        page_url = url_lookup[fname]["url"]
                        thumb_path = thumb_paths.get(fname, "")
                        conn.execute(
                            "UPDATE processed_pdfs SET url = ?, thumbnail_path = ? WHERE pdf_filename = ?",
                            (page_url, thumb_path or None, fname)
                        )
                        matched += 1

                conn.commit()
                urls_fixed += matched
                not_matched += len(needed) - matched

                thumbs_got = sum(1 for f in needed if thumb_paths.get(f))
                if matched < len(needed):
                    log.info(f"    Matched {matched}/{len(needed)} — {len(needed) - matched} not found | {thumbs_got} thumbs")
                else:
                    log.info(f"    Matched all {matched} | {thumbs_got} thumbs")

                searches_done += 1

            except Exception as e:
                log.error(f"    Error: {e}")

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    finally:
        if driver and not keep_browser_open:
            try:
                driver.quit()
            except Exception:
                pass
        elif keep_browser_open:
            log.info("Browser left open for user to resolve challenge.")

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Searches done: {searches_done}")
    log.info(f"  URLs fixed: {urls_fixed}")
    log.info(f"  Not matched: {not_matched}")
    log.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    main(max_searches=limit)
