"""Backfill missing thumbnails by visiting search result pages.

Groups entries by date, visits the search page for each date,
grabs the thumbnail images displayed in the results, matches
them to DB entries by image ID, and saves them.

Usage:
    python backfill_thumbnails.py --year 1923
    python backfill_thumbnails.py --date-start 1923-01-01 --date-end 1923-12-31
"""

import argparse
import base64
import os
import re
import time
import sqlite3
import logging
from pathlib import Path
from collections import defaultdict

from seleniumbase import Driver
from selenium.webdriver.common.by import By

DB_PATH = r"c:\lake_worth\lake_worth.db"
THUMB_DIR = Path(r"c:\lake_worth\thumbnails")
RUNTIME_DIR = Path(r"c:\lake_worth_runtime")
PROFILE_DIR = RUNTIME_DIR / "chrome_temp_profile_backfill"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("backfill_thumbs")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_missing(conn, date_start, date_end):
    return conn.execute("""
        SELECT pdf_filename, url, date_str, search_term FROM processed_pdfs
        WHERE (thumbnail_path IS NULL OR thumbnail_path = '')
          AND url IS NOT NULL AND url != ''
          AND date_str >= ? AND date_str <= ?
        ORDER BY date_str
    """, (date_start, date_end)).fetchall()


def extract_image_id(url):
    m = re.search(r'/image/(\d+)', url)
    return m.group(1) if m else None


def build_search_url(date_str, term="lake worth"):
    """Build a search URL for a single date."""
    return (
        f"https://star-telegram.newspapers.com/search/results/"
        f"?date-start={date_str}&date-end={date_str}"
        f"&keyword=%22{term.replace(' ', '+')}%22&sort=paper-date-asc"
    )


def collect_thumbnails_from_page(driver):
    """Scrape thumbnail URLs from current search results page.
    Returns dict: image_id -> thumb_src_url
    """
    return driver.execute_script("""
        var result = {};
        var imgs = document.querySelectorAll('img[src*="/img/thumbnail/"]');
        imgs.forEach(function(img) {
            var src = img.getAttribute('src') || '';
            var m = src.match(/\\/img\\/thumbnail\\/(\\d+)\\//);
            if (m) result[m[1]] = src;
        });
        return result;
    """) or {}


def download_thumb_batch(driver, to_fetch):
    """Download thumbnail images via browser JS fetch.
    to_fetch: list of (pdf_filename, thumb_src_url)
    Returns dict: pdf_filename -> relative path saved
    """
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
        log.warning(f"    Batch fetch failed: {e}")
        return {}

    results = {}
    for i, (fname, _) in enumerate(to_fetch):
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
                log.warning(f"    Thumbnail save failed for {fname}: {e}")
    return results


def click_show_more(driver):
    """Click 'Show More' button if present."""
    try:
        buttons = driver.find_elements(By.CSS_SELECTOR, "button, a, [role='button']")
        for el in buttons:
            try:
                text = el.text.strip().lower()
                if "show more" in text or "load more" in text:
                    if el.is_displayed() and el.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(1)
                        el.click()
                        time.sleep(4)
                        return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="Year (shorthand for full year range)")
    parser.add_argument("--date-start", help="Start date YYYY-MM-DD")
    parser.add_argument("--date-end", help="End date YYYY-MM-DD")
    args = parser.parse_args()

    if args.date_start and args.date_end:
        date_start, date_end = args.date_start, args.date_end
    elif args.year:
        date_start = f"{args.year}-01-01"
        date_end = f"{args.year}-12-31"
    else:
        parser.error("Provide --year or --date-start and --date-end")

    conn = get_db()
    rows = get_missing(conn, date_start, date_end)
    log.info(f"Found {len(rows)} entries missing thumbnails ({date_start} to {date_end})")
    if not rows:
        return

    # Group by (date, search_term) — one search page per group
    groups = defaultdict(list)  # (date_str, term) -> [(pdf_filename, image_id)]
    for r in rows:
        img_id = extract_image_id(r["url"])
        if img_id:
            term = r["search_term"] or "lake worth"
            groups[(r["date_str"], term)].append((r["pdf_filename"], img_id))

    log.info(f"  {len(groups)} search pages to visit")

    # Setup browser
    THUMB_DIR.mkdir(parents=True, exist_ok=True)
    os.makedirs(PROFILE_DIR, exist_ok=True)

    log.info("Starting browser...")
    driver = Driver(uc=True, headed=True, user_data_dir=str(PROFILE_DIR),
                    chromium_arg="--disable-session-crashed-bubble")
    try:
        driver.maximize_window()
    except Exception:
        pass

    driver.uc_open_with_reconnect("https://star-telegram.newspapers.com/", 4)
    time.sleep(3)

    # Check if logged in
    page_text = driver.execute_script("return document.body.innerText || '';").lower()
    if "sign in" in page_text or "log in" in page_text:
        log.info("Not logged in. Please log in manually in the browser window.")
        log.info("Waiting up to 120 seconds...")
        for i in range(120):
            time.sleep(1)
            try:
                pt = driver.execute_script("return document.body.innerText || '';").lower()
                if "sign in" not in pt and "log in" not in pt:
                    log.info("Login detected!")
                    break
            except Exception:
                pass
        else:
            log.error("Login timeout. Exiting.")
            driver.quit()
            return
    else:
        log.info("Already logged in.")

    time.sleep(2)
    total_saved = 0
    total_needed = sum(len(v) for v in groups.values())

    for idx, ((date_str, term), entries) in enumerate(sorted(groups.items())):
        needed_ids = {img_id: fname for fname, img_id in entries}
        search_url = build_search_url(date_str, term)
        log.info(f"  [{idx + 1}/{len(groups)}] {date_str} \"{term}\" — {len(entries)} entries")

        try:
            driver.get(search_url)
        except Exception:
            try:
                driver.execute_script(f"window.location.href = '{search_url}';")
            except Exception as e:
                log.warning(f"    Navigation failed: {e}")
                continue
        time.sleep(4)

        # Collect thumbnails, clicking "Show More" if needed
        thumb_lookup = collect_thumbnails_from_page(driver)
        attempts = 0
        while len(thumb_lookup) < len(needed_ids) and attempts < 5:
            if not click_show_more(driver):
                break
            thumb_lookup.update(collect_thumbnails_from_page(driver))
            attempts += 1

        # Match found thumbnails to our entries
        to_fetch = []
        for img_id, fname in needed_ids.items():
            if img_id in thumb_lookup:
                to_fetch.append((fname, thumb_lookup[img_id]))

        if not to_fetch:
            log.info(f"    No thumbnails found on search page")
            continue

        # Download and save
        saved = download_thumb_batch(driver, to_fetch)
        for fname, thumb_path in saved.items():
            conn.execute(
                "UPDATE processed_pdfs SET thumbnail_path = ? WHERE pdf_filename = ?",
                (thumb_path, fname)
            )
        conn.commit()

        total_saved += len(saved)
        log.info(f"    Saved {len(saved)}/{len(entries)} thumbnails (total: {total_saved}/{total_needed})")
        time.sleep(1)

    log.info(f"Done. Saved {total_saved}/{total_needed} thumbnails.")
    driver.quit()
    conn.close()


if __name__ == "__main__":
    main()
