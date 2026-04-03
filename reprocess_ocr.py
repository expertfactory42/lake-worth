"""
Reprocess clipped pages that have bad/missing OCR.

Visits each existing clip URL, clicks the OCR button, waits for text,
then runs through Claude to extract articles. Does NOT re-clip.

Usage:
    python reprocess_ocr.py [max_pages]
"""

import os
import re
import sys
import time
import sqlite3
import logging
from datetime import datetime

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

import undetected_chromedriver as uc

# Reuse functions from clip_and_extract
sys.path.insert(0, os.path.dirname(__file__))
from selenium.webdriver.common.by import By
from clip_and_extract import (
    DB_PATH, SEARCH_TERM,
    _click_ocr_button, _wait_for_ocr_text, extract_ocr_text,
    extract_articles_with_ai, save_articles,
)

MIN_CLIP_WIDTH = 750
MIN_CLIP_HEIGHT = 800

# === STOP FLAG ===
STOP_FLAG_FILE = r"c:\lake_worth\stop_reprocessor"


def check_stop_flag():
    """Check if stop flag file exists. Returns True if script should stop."""
    if os.path.exists(STOP_FLAG_FILE):
        log.info("  Stop flag detected — exiting gracefully.")
        return True
    return False

# === LOGGING ===
LOG_DIR = r"c:\lake_worth\collector_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"reprocess_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("reprocess")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_pages_to_reprocess(conn):
    """Get clipped pages with bad/missing OCR that have a clip URL."""
    rows = conn.execute("""
        SELECT pp.pdf_filename, pp.clip_url
        FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1 AND pp.clipped = 1
        AND (pp.ignored IS NULL OR pp.ignored = 0)
        AND (pp.ocr_text IS NULL OR length(pp.ocr_text) <= 1000)
        AND pp.clip_url IS NOT NULL AND pp.clip_url != ''
        ORDER BY pp.pdf_filename
    """).fetchall()
    return rows


def parse_filename(fname):
    m = re.match(
        r"^(?P<paper>.+)_(?P<y>\d{4})_(?P<m>\d{2})_(?P<d>\d{2})_(?P<p>\d+)\.pdf$",
        fname, re.IGNORECASE
    )
    if not m:
        return None
    return {
        "newspaper": m.group("paper").replace("_", " "),
        "date": f"{m.group('y')}-{m.group('m')}-{m.group('d')}",
        "page": m.group("p"),
    }


def setup_driver():
    temp_profile = r"c:\lake_worth\chrome_temp_profile_clipper"
    os.makedirs(temp_profile, exist_ok=True)
    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=146)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)

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


def main(max_pages=0):
    conn = get_db()
    pages = get_pages_to_reprocess(conn)

    log.info("=" * 60)
    log.info("OCR Reprocessor — Extract from existing clips")
    log.info("=" * 60)
    log.info(f"  Pages to reprocess: {len(pages)}")
    log.info(f"  Max pages: {max_pages or 'unlimited'}")
    log.info(f"  Log: {log_filename}")

    if not pages:
        log.info("Nothing to reprocess.")
        return

    # Clear stop flag from previous runs
    if os.path.exists(STOP_FLAG_FILE):
        os.remove(STOP_FLAG_FILE)
        log.info("  Cleared old stop flag.")

    driver = setup_driver()

    processed = 0
    articles_found = 0
    errors = 0
    bad_clips = 0
    keep_browser_open = False

    try:
        for row in pages:
            if check_stop_flag():
                break
            if max_pages and processed >= max_pages:
                log.info(f"Reached max_pages limit ({max_pages})")
                break

            fname = row["pdf_filename"]
            clip_url = row["clip_url"]
            meta = parse_filename(fname)
            if not meta:
                log.warning(f"  SKIP (bad filename): {fname}")
                continue

            page_start = time.time()
            log.info(f"  [{processed + 1}/{len(pages)}] {fname}")
            log.info(f"    Clip: {clip_url}")

            try:
                # Navigate to clip page
                driver.get(clip_url)
                time.sleep(5)

                # Check for non-public clipping
                page_text = driver.execute_script("return document.body.innerText || '';")
                if "not public" in page_text.lower():
                    log.warning(f"    NOT PUBLIC — skipping.")
                    conn.execute(
                        "UPDATE processed_pdfs SET clipped = 0 WHERE pdf_filename = ?",
                        (fname,)
                    )
                    conn.commit()
                    bad_clips += 1
                    continue

                # Check for 404 / Not Found page
                page_text = driver.execute_script("return document.body.innerText || '';")
                if "not found" in page_text.lower() or "404" in driver.title.lower():
                    log.warning(f"    PAGE NOT FOUND — marking for re-clip.")
                    conn.execute(
                        "UPDATE processed_pdfs SET clipped = 0 WHERE pdf_filename = ?",
                        (fname,)
                    )
                    conn.commit()
                    bad_clips += 1
                    continue

                # Check clip image size
                try:
                    clip_img = driver.find_element(By.CSS_SELECTOR, "img[src*='clippingId'], img[src*='clip'], img.article-image, main img")
                    img_width = int(clip_img.get_attribute("naturalWidth") or clip_img.get_attribute("width") or 0)
                    img_height = int(clip_img.get_attribute("naturalHeight") or clip_img.get_attribute("height") or 0)
                    log.info(f"    Clip size: {img_width}x{img_height}")
                    if img_width < MIN_CLIP_WIDTH or img_height < MIN_CLIP_HEIGHT or img_height <= img_width:
                        log.warning(f"    BAD CLIP — too small ({img_width}x{img_height}). Marking for re-clip.")
                        conn.execute(
                            "UPDATE processed_pdfs SET clipped = 0 WHERE pdf_filename = ?",
                            (fname,)
                        )
                        conn.commit()
                        bad_clips += 1
                        continue
                except Exception as e:
                    log.warning(f"    Could not check clip size: {e}")

                # Check if OCR button exists before attempting extraction
                from selenium.webdriver.common.by import By as _By
                ocr_btn_found = False
                for _ in range(3):
                    try:
                        elements = driver.find_elements(_By.XPATH, "//*[contains(text(), 'Article Text')]")
                        for el in elements:
                            if el.is_displayed():
                                ocr_btn_found = True
                                break
                    except Exception:
                        pass
                    if ocr_btn_found:
                        break
                    time.sleep(2)

                if not ocr_btn_found:
                    log.warning(f"    NO OCR BUTTON — possible Cloudflare challenge. Stopping. Browser left open.")
                    keep_browser_open = True
                    break

                # Extract OCR text (clicks button, waits, retries — 4 attempts)
                ocr_text = extract_ocr_text(driver)
                log.info(f"    OCR: {len(ocr_text)} chars")

                # If still too short, refresh page and try 3 more times
                if len(ocr_text) <= 1000:
                    log.info(f"    OCR too short after initial tries. Refreshing page...")
                    for refresh_try in range(3):
                        driver.refresh()
                        time.sleep(5)
                        ocr_text = extract_ocr_text(driver)
                        log.info(f"    OCR after refresh {refresh_try + 1}/3: {len(ocr_text)} chars")
                        if len(ocr_text) > 1000:
                            break

                if len(ocr_text) <= 1000:
                    log.warning(f"    OCR still too short ({len(ocr_text)} chars). Skipping.")
                    errors += 1
                    continue

                # Update OCR text in DB
                conn.execute(
                    "UPDATE processed_pdfs SET ocr_text = ? WHERE pdf_filename = ?",
                    (ocr_text, fname)
                )
                conn.commit()

                # Run through Claude
                articles = extract_articles_with_ai(
                    ocr_text, meta["date"], meta["newspaper"], meta["page"]
                )

                if articles:
                    count = save_articles(conn, fname, articles, SEARCH_TERM, clip_url=clip_url)
                    conn.execute(
                        "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
                        (count, fname)
                    )
                    conn.commit()
                    articles_found += count
                    log.info(f"    Found {count} articles")
                else:
                    log.info(f"    No Lake Worth articles found")

                elapsed = time.time() - page_start
                log.info(f"    {fname} — {elapsed:.1f}s ({len(articles)} articles)")
                processed += 1

            except Exception as e:
                log.error(f"    Error: {e}")
                errors += 1

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
    log.info(f"  Pages reprocessed: {processed}")
    log.info(f"  Articles found: {articles_found}")
    log.info(f"  Bad clips (marked for re-clip): {bad_clips}")
    log.info(f"  Errors: {errors}")
    log.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    main(max_pages=limit)
