"""
Automated full-page clipper for newspapers.com.

Visits each search result page, creates a full-page clip to trigger OCR,
then extracts the OCR text and feeds it to Claude Haiku to find
"lake worth" articles.

Workflow per page:
  1. Navigate to image page
  2. Zoom out (Ctrl+- x7) to see full page
  3. Click "Clip" button
  4. Drag clip box corners to cover full page
  5. Click "Save"
  6. Click "View Clip"
  7. Scrape OCR text + clip URL
  8. Send OCR to Claude Haiku for article extraction
  9. Save articles to DB

Usage:
    python clip_and_extract.py [max_pages]
"""

import os
import re
import sys
import time
import json
import sqlite3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, StaleElementReferenceException,
    NoSuchElementException, ElementNotInteractableException,
    WebDriverException, InvalidSessionIdException,
)

# === CONFIGURATION ===
DB_PATH = r"c:\lake_worth\lake_worth.db"
LOG_DIR = r"c:\lake_worth\collector_logs"
DATE_END = "1925-12-31"
SEARCH_TERM = "lake worth"
VIEWS_PER_HOUR = 450  # Stay under 500 to be safe
ZOOM_OUT_TIMES = 9
WAIT_TIMEOUT = 15
ACTION_DELAY = 2

# === LOGGING ===
os.makedirs(LOG_DIR, exist_ok=True)
log_filename = os.path.join(LOG_DIR, f"clipper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler(),
    ],
)
log = logging.getLogger("clipper")

MONTH_MAP = {
    "jan": "01", "feb": "02", "mar": "03", "apr": "04",
    "may": "05", "jun": "06", "jul": "07", "aug": "08",
    "sep": "09", "oct": "10", "nov": "11", "dec": "12"
}


# === DATABASE ===

def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_columns(conn):
    """Add columns if they don't exist yet."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(processed_pdfs)").fetchall()}
    if "ocr_text" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN ocr_text TEXT")
        conn.commit()
        log.info("Added ocr_text column to processed_pdfs")
    if "clip_url" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN clip_url TEXT")
        conn.commit()
        log.info("Added clip_url column to processed_pdfs")
    if "clipped" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN clipped INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added clipped column to processed_pdfs")

    # Articles table
    art_cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()}
    if "has_photo" not in art_cols:
        conn.execute("ALTER TABLE articles ADD COLUMN has_photo INTEGER DEFAULT 0")
        conn.commit()
        log.info("Added has_photo column to articles")
    if "photo_description" not in art_cols:
        conn.execute("ALTER TABLE articles ADD COLUMN photo_description TEXT")
        conn.commit()
        log.info("Added photo_description column to articles")


def get_start_date(conn):
    """Read the clipper date counter from DB. Defaults to 1914-01-01."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clipper_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    conn.commit()
    row = conn.execute(
        "SELECT value FROM clipper_state WHERE key = 'current_date'"
    ).fetchone()
    if row and row[0]:
        return row[0]
    return "1914-01-01"


def save_start_date(conn, date_str):
    """Update the clipper date counter in DB."""
    conn.execute("""
        INSERT INTO clipper_state (key, value) VALUES ('current_date', ?)
        ON CONFLICT(key) DO UPDATE SET value = ?
    """, (date_str, date_str))
    conn.commit()


def needs_clipping(conn, pdf_filename):
    """Check if this entry needs clipping.

    Skip if already clipped by us, or if the user has already manually
    clipped it (has_image=1 in articles table).
    """
    row = conn.execute(
        "SELECT clipped FROM processed_pdfs WHERE pdf_filename = ?", (pdf_filename,)
    ).fetchone()
    if row is not None and row["clipped"] == 1:
        return False

    # Check if user already clipped this page (has_image=1 in articles)
    img_row = conn.execute(
        "SELECT 1 FROM articles WHERE pdf_filename = ? AND has_image = 1 LIMIT 1",
        (pdf_filename,)
    ).fetchone()
    if img_row:
        return False

    return True


def backfill_page_url(conn, pdf_filename, url):
    """Store the original page URL for an already-processed entry.

    If the entry exists in processed_pdfs but has no URL, fill it in.
    If the entry doesn't exist (old extraction path), create it with clipped=1.
    """
    row = conn.execute(
        "SELECT url FROM processed_pdfs WHERE pdf_filename = ?", (pdf_filename,)
    ).fetchone()
    if row is None:
        # Entry doesn't exist — create it but don't mark as clipped yet
        conn.execute(
            "INSERT INTO processed_pdfs (pdf_filename, url, clipped, search_term) VALUES (?, ?, 0, ?)",
            (pdf_filename, url, SEARCH_TERM)
        )
        conn.commit()
    elif not row[0]:
        # Entry exists but URL is missing — fill it in
        conn.execute(
            "UPDATE processed_pdfs SET url = ? WHERE pdf_filename = ?",
            (url, pdf_filename)
        )
        conn.commit()


def save_clip_data(conn, pdf_filename, url, clip_url, ocr_text):
    """Save clip results to DB."""
    conn.execute(
        """UPDATE processed_pdfs
           SET url = ?, clip_url = ?, ocr_text = ?, clipped = 1
           WHERE pdf_filename = ?""",
        (url, clip_url, ocr_text, pdf_filename)
    )
    conn.commit()


def save_articles(conn, pdf_filename, articles, search_term, clip_url=""):
    """Save extracted articles to the articles table."""
    # Parse date/newspaper/page from filename
    m = re.search(r'(.+?)_(\d{4})_(\d{2})_(\d{2})_(\d+)\.pdf$', pdf_filename)
    if not m:
        return 0
    newspaper = m.group(1).replace("_", " ")
    date_str = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    page = int(m.group(5))

    # Extract clip_id from clip URL
    clip_id = ""
    if clip_url:
        cm = re.search(r'/(\d+)/?$', clip_url)
        if cm:
            clip_id = cm.group(1)

    count = 0
    for article in articles:
        headline = article.get("headline", "").strip()
        text = article.get("text", "").strip()
        photo_desc = (article.get("photo_description") or "").strip()
        has_photo = 1 if photo_desc else 0
        if not headline and not text:
            continue

        # Dedup check: skip if substantially same text already exists for this date+page
        from difflib import SequenceMatcher
        existing = conn.execute(
            "SELECT id, full_text FROM articles WHERE date = ? AND page = ?",
            (date_str, page)
        ).fetchall()
        is_dupe = False
        replace_id = None
        for ex_id, ex_text in existing:
            ex_text = (ex_text or "")
            ratio = SequenceMatcher(None, text[:500].lower(), ex_text[:500].lower()).ratio()
            if ratio > 0.5:
                if len(text) > len(ex_text):
                    replace_id = ex_id
                    log.info(f"    Replacing shorter duplicate (id={ex_id}, {ratio:.0%} match)")
                else:
                    is_dupe = True
                    log.info(f"    Skipping duplicate (id={ex_id}, {ratio:.0%} match)")
                break
        if is_dupe:
            continue
        if replace_id:
            conn.execute(
                """UPDATE articles SET headline=?, full_text=?, pdf_filename=?, search_term=?, clip_id=?, has_photo=?, photo_description=?
                   WHERE id=?""",
                (headline, text, pdf_filename, search_term, clip_id or None, has_photo, photo_desc or None, replace_id)
            )
        else:
            conn.execute(
                """INSERT INTO articles (date, newspaper, page, headline, full_text, pdf_filename, search_term, has_image, clip_id, has_photo, photo_description)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?)""",
                (date_str, newspaper, page, headline, text, pdf_filename, search_term, clip_id or None, has_photo, photo_desc or None)
            )
        if has_photo:
            log.info(f"    >>> PHOTO: {photo_desc[:60]}")
        count += 1

    if count > 0:
        conn.execute(
            "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
            (count, pdf_filename)
        )
        conn.commit()
    return count


# === PAGE TITLE PARSER ===

def parse_page_title(title, url):
    """Parse the image page title into metadata."""
    newspaper = "Fort_Worth_Star_Telegram"
    m = re.search(
        r'(Fort Worth (?:Star-Telegram|Record-Telegram|Record Telegram|Star Telegram|Record))',
        title, re.IGNORECASE
    )
    if m:
        newspaper = re.sub(r'[^a-zA-Z0-9]+', '_', m.group(1)).strip('_')

    date_match = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', title)
    date_str = ""
    if date_match:
        month_str = date_match.group(1)[:3].lower()
        month = MONTH_MAP.get(month_str, "00")
        day = date_match.group(2).zfill(2)
        year = date_match.group(3)
        date_str = f"{year}-{month}-{day}"

    page_match = re.search(r'page\s*(\d+)', title, re.IGNORECASE)
    page = int(page_match.group(1)) if page_match else 0

    if date_str and page:
        pdf_filename = f"{newspaper}_{date_str.replace('-', '_')}_{page}.pdf"
    else:
        img_match = re.search(r'/image/(\d+)', url)
        img_id = img_match.group(1) if img_match else str(int(time.time()))
        pdf_filename = f"{newspaper}_{img_id}.pdf"

    return {
        "newspaper": newspaper,
        "date": date_str,
        "page": page,
        "pdf_filename": pdf_filename,
        "url": url,
    }


# === RATE LIMITING ===

_view_times = []


def track_view():
    """Record a page view and pause if we've hit the hourly limit."""
    now = time.time()
    _view_times.append(now)
    cutoff = now - 3600
    while _view_times and _view_times[0] < cutoff:
        _view_times.pop(0)

    if len(_view_times) >= VIEWS_PER_HOUR:
        oldest = _view_times[0]
        wait_until = oldest + 3600
        wait_secs = wait_until - now
        if wait_secs > 0:
            log.info(f"  >>> THROTTLE: {len(_view_times)} views/hr. Pausing {int(wait_secs)}s...")
            time.sleep(wait_secs)
            now2 = time.time()
            cutoff2 = now2 - 3600
            while _view_times and _view_times[0] < cutoff2:
                _view_times.pop(0)


# === BROWSER SETUP ===

def setup_driver():
    options = uc.ChromeOptions()
    temp_profile = r"c:\lake_worth\chrome_temp_profile_clipper"
    os.makedirs(temp_profile, exist_ok=True)
    options.add_argument(f"--user-data-dir={temp_profile}")
    driver = uc.Chrome(options=options, version_main=146)
    driver.set_window_size(1920, 1080)
    driver.implicitly_wait(5)

    # Check if logged in — if not, pause for manual login
    driver.get("https://star-telegram.newspapers.com/")
    time.sleep(5)
    page_text = driver.execute_script("return document.body.innerText || '';").lower()
    if "sign in" in page_text or "log in" in page_text:
        log.info("NOT LOGGED IN — please log in to newspapers.com in the browser window.")
        log.info("Waiting up to 30 seconds for login...")
        for i in range(30):
            time.sleep(1)
            try:
                page_text = driver.execute_script("return document.body.innerText || '';").lower()
                if "sign in" not in page_text and "log in" not in page_text:
                    log.info("Login detected! Continuing...")
                    break
            except Exception:
                pass
        else:
            log.warning("Login timeout — proceeding anyway.")
    else:
        log.info("Already logged in.")

    return driver


# === SEARCH RESULTS ===

def collect_search_results(driver):
    """Collect result links and metadata from current search results page.

    Returns list of dicts: {"url": ..., "text": ..., "date": ..., "page": ..., "pdf_filename": ...}
    The text/date/page are parsed from the search result listing (no click needed).
    """
    results = []
    try:
        WebDriverWait(driver, WAIT_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "a[href*='/image/']"))
        )
        time.sleep(ACTION_DELAY)
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/image/']")
        # Collect all links, preferring ones with text (two links per result: image + text)
        url_data = {}  # href -> best text
        for link in links:
            try:
                href = link.get_attribute("href")
                if href and "/image/" in href:
                    text = link.text.strip()
                    # Keep the version with the most text
                    if href not in url_data or len(text) > len(url_data[href]):
                        url_data[href] = text
            except StaleElementReferenceException:
                continue
        for href, text in url_data.items():
            meta = parse_page_title(text, href)
            results.append({
                "url": href,
                "text": text,
                "date": meta["date"],
                "page": meta["page"],
                "pdf_filename": meta["pdf_filename"],
            })
    except TimeoutException:
        log.info("  No results found on page.")
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
                            time.sleep(4)
                            return True
                except StaleElementReferenceException:
                    continue
    except Exception:
        pass
    return False


# === CLIPPING ===

def zoom_out(driver, times=ZOOM_OUT_TIMES):
    """Zoom out the page using Ctrl+minus."""
    body = driver.find_element(By.TAG_NAME, "body")
    for _ in range(times):
        body.send_keys(Keys.CONTROL, "-")
        time.sleep(0.3)
    time.sleep(1)


def click_clip_button(driver):
    """Find and click the Clip button on the image viewer page."""
    # Try multiple approaches to find the clip button
    selectors = [
        # Button with text "Clip"
        "//button[contains(text(), 'Clip')]",
        "//a[contains(text(), 'Clip')]",
        # Button with clip icon/class
        "//button[contains(@class, 'clip')]",
        "//a[contains(@class, 'clip')]",
        # aria-label
        "//*[@aria-label='Clip']",
        "//*[@aria-label='clip']",
        # data attributes
        "//*[contains(@data-action, 'clip')]",
        # Title attribute
        "//*[@title='Clip']",
        "//*[@title='Create clip']",
    ]

    for xpath in selectors:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                if el.is_displayed():
                    el.click()
                    log.info("    Clicked Clip button")
                    time.sleep(2)
                    return True
        except Exception:
            continue

    # Fallback: look for any clickable element with "clip" in text
    try:
        elements = driver.execute_script("""
            var all = document.querySelectorAll('button, a, [role="button"], [class*="clip"], [id*="clip"]');
            var results = [];
            for (var i = 0; i < all.length; i++) {
                var el = all[i];
                var text = (el.textContent || '').trim().toLowerCase();
                var cls = (el.className || '').toLowerCase();
                var id = (el.id || '').toLowerCase();
                if (text.includes('clip') || cls.includes('clip') || id.includes('clip')) {
                    results.push({
                        tag: el.tagName,
                        text: text.substring(0, 50),
                        cls: cls.substring(0, 80),
                        id: id,
                        visible: el.offsetParent !== null
                    });
                }
            }
            return results;
        """)
        if elements:
            log.info(f"    Found clip-related elements: {elements}")
    except Exception:
        pass

    return False


def drag_clip_corners(driver):
    """Drag the clip box corners to cover the full page.

    The clip box has 4 SVG circle handles identified by cursor style:
      nw-resize (upper-left), ne-resize (upper-right),
      sw-resize (lower-left), se-resize (lower-right).
    The viewer area is an SVG element with id="svg-viewer".
    """
    # Retry finding handles — they may take a moment to appear
    info = None
    for attempt in range(10):
        time.sleep(1)
        info = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var handles = {};
        for (var i = 0; i < circles.length; i++) {
            var c = circles[i];
            var style = window.getComputedStyle(c);
            var rect = c.getBoundingClientRect();
            if (rect.width < 1) continue;
            var cursor = style.cursor;
            if (cursor === 'nw-resize' || cursor === 'ne-resize' ||
                cursor === 'sw-resize' || cursor === 'se-resize') {
                handles[cursor] = {
                    x: rect.left + rect.width/2,
                    y: rect.top + rect.height/2
                };
            }
        }
        var viewer = document.getElementById('svg-viewer');
        var vr = viewer ? viewer.getBoundingClientRect() : null;
        return {
            handles: handles,
            viewer: vr ? {left: vr.left, top: vr.top, right: vr.right, bottom: vr.bottom} : null
        };
    """)

        handles = info.get("handles", {})
        viewer = info.get("viewer")
        if "nw-resize" in handles and "se-resize" in handles and viewer:
            break
        if attempt < 9:
            log.info(f"    Waiting for clip handles (attempt {attempt+1})...")

    if "nw-resize" not in handles or "se-resize" not in handles:
        log.warning(f"    Could not find clip handles after 10 attempts. Found: {list(handles.keys())}")
        return False

    if not viewer:
        log.warning("    Could not find svg-viewer element")
        return False

    nw = handles["nw-resize"]
    se = handles["se-resize"]

    log.info(f"    Viewer bounds: ({viewer['left']:.0f},{viewer['top']:.0f}) to ({viewer['right']:.0f},{viewer['bottom']:.0f})")
    log.info(f"    NW handle at ({nw['x']:.0f},{nw['y']:.0f}), SE handle at ({se['x']:.0f},{se['y']:.0f})")

    margin = 10

    # Find the actual circle elements to use as anchors
    nw_el = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'nw-resize') return circles[i];
        }
        return null;
    """)
    se_el = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'se-resize') return circles[i];
        }
        return null;
    """)

    if not nw_el or not se_el:
        log.warning("    Could not find circle elements")
        return False

    # Drag NW handle to top-left of viewer
    dx_nw = int(viewer["left"] + margin - nw["x"])
    dy_nw = int(viewer["top"] + margin - nw["y"])
    log.info(f"    Dragging NW by ({dx_nw}, {dy_nw})")
    actions = ActionChains(driver)
    actions.click_and_hold(nw_el)
    actions.pause(0.2)
    actions.move_by_offset(dx_nw, dy_nw)
    actions.pause(0.2)
    actions.release()
    actions.perform()
    time.sleep(2)
    log.info("    Dragged NW handle to top-left")

    # Re-find SE handle position and drag via JavaScript mouse events
    result = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var se = null;
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'se-resize') {
                se = circles[i];
                break;
            }
        }
        if (!se) return {error: 'no se handle'};

        var rect = se.getBoundingClientRect();
        var startX = rect.left + rect.width/2;
        var startY = rect.top + rect.height/2;

        var viewer = document.getElementById('svg-viewer');
        var vr = viewer.getBoundingClientRect();
        var endX = vr.right - """ + str(margin) + """;
        var endY = vr.bottom - """ + str(margin) + """;

        // Dispatch mouse events directly
        function fireMouseEvent(type, x, y) {
            var evt = new MouseEvent(type, {
                bubbles: true, cancelable: true, view: window,
                clientX: x, clientY: y,
                button: 0, buttons: 1
            });
            se.dispatchEvent(evt);
        }

        fireMouseEvent('mousedown', startX, startY);

        // Move in steps for smooth drag
        var steps = 10;
        for (var s = 1; s <= steps; s++) {
            var mx = startX + (endX - startX) * s / steps;
            var my = startY + (endY - startY) * s / steps;
            fireMouseEvent('mousemove', mx, my);
        }

        fireMouseEvent('mouseup', endX, endY);

        return {
            start: {x: startX, y: startY},
            end: {x: endX, y: endY},
            ok: true
        };
    """)

    if not result or result.get("error"):
        log.warning(f"    SE drag failed: {result}")
        return False

    log.info(f"    Dragged SE via JS: ({result['start']['x']:.0f},{result['start']['y']:.0f}) -> ({result['end']['x']:.0f},{result['end']['y']:.0f})")
    time.sleep(2)

    return True


def click_save_button(driver):
    """Click the Save button after positioning the clip."""
    selectors = [
        "//button[contains(text(), 'Save')]",
        "//a[contains(text(), 'Save')]",
        "//button[contains(@class, 'save')]",
        "//*[@aria-label='Save']",
        "//input[@type='submit' and contains(@value, 'Save')]",
    ]
    for xpath in selectors:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                if el.is_displayed() and el.is_enabled():
                    el.click()
                    log.info("    Clicked Save button")
                    time.sleep(3)
                    return True
        except Exception:
            continue
    return False


def navigate_to_clip_page(driver):
    """After saving a clip, navigate to the clip/article page to get OCR text.

    After save, the URL updates to include clipping_id parameter.
    We can also find the article link in the page.
    """
    time.sleep(2)

    # Method 1: Get clipping_id from URL and find the article link
    current = driver.current_url
    m = re.search(r'clipping_id=(\d+)', current)
    if m:
        clip_id = m.group(1)
        # Look for article link containing this clip ID
        article_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/article/']")
        for link in article_links:
            href = link.get_attribute("href") or ""
            if clip_id in href:
                driver.get(href)
                log.info(f"    Navigated to clip article: {href[:80]}")
                time.sleep(5)
                return True

        # Construct URL directly from what we know
        # Pattern: /article/NEWSPAPER_SLUG/CLIPPING_ID/
        # Try navigating via the clipping page
        article_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/article/']")
        if article_links:
            href = article_links[0].get_attribute("href")
            if href:
                driver.get(href)
                log.info(f"    Navigated to article link: {href[:80]}")
                time.sleep(5)
                return True

    # Method 2: Try clicking View Clip button quickly
    for xpath in ["//button[contains(text(), 'View Clip')]",
                  "//a[contains(text(), 'View Clip')]"]:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                if el.is_displayed():
                    el.click()
                    log.info("    Clicked View Clip button")
                    time.sleep(5)
                    return True
        except Exception:
            continue

    return False


def _click_ocr_button(driver):
    """Click the 'Show Article Text (OCR)' button. Returns True if clicked."""
    for attempt in range(5):
        try:
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Article Text')]")
            for el in elements:
                el_text = (el.text or "").strip().lower()
                if el.is_displayed() and "show" in el_text and "hide" not in el_text:
                    el.click()
                    log.info(f"    Clicked: {el.text.strip()[:40]}")
                    return True
        except Exception:
            pass
        time.sleep(1)
    return False


def _wait_for_ocr_text(driver):
    """Wait for OCR text to stabilize, then extract it. Returns text string."""
    time.sleep(3)
    prev_len = 0
    stable_count = 0
    for wait in range(30):  # up to 30 seconds
        body_text = driver.execute_script("""
            var main = document.querySelector('main, [role="main"]');
            return main ? main.innerText : document.body.innerText;
        """) or ""
        cur_len = len(body_text)
        if cur_len == prev_len:
            stable_count += 1
            if stable_count >= 3 and cur_len > 2000:
                break
            if stable_count >= 5:
                break
        else:
            stable_count = 0
        prev_len = cur_len
        time.sleep(1)

    # Extract the OCR text from specific selectors
    text = ""
    selectors = [
        "[class*='ocr']",
        "[class*='transcription']",
        "[class*='article-text']",
        "[class*='clip-text']",
        "[class*='text-content']",
        "pre",
    ]
    for sel in selectors:
        elements = driver.find_elements(By.CSS_SELECTOR, sel)
        for el in elements:
            t = el.text.strip()
            if len(t) > len(text):
                text = t

    # Fallback: grab main content text
    if len(text) < 500:
        text = driver.execute_script("""
            var main = document.querySelector('main, [role="main"]');
            if (main) return main.innerText;
            return document.body.innerText;
        """) or ""

    return text.strip()


def extract_ocr_text(driver):
    """Extract OCR text from the clip viewing page.

    Clicks the OCR button, waits for text. If text is under 2000 chars,
    re-clicks the OCR button up to 4 times to get better results.
    """
    text = ""
    try:
        # Check if OCR text is already visible (button may have auto-expanded)
        already_visible = False
        try:
            for sel in ["[class*='ocr']", "[class*='transcription']"]:
                els = driver.find_elements(By.CSS_SELECTOR, sel)
                for el in els:
                    if el.is_displayed() and len(el.text.strip()) > 200:
                        already_visible = True
                        break
                if already_visible:
                    break
        except Exception:
            pass

        if already_visible:
            log.info("    OCR text already visible")
        else:
            if not _click_ocr_button(driver):
                log.warning("    Could not find Show Article Text (OCR) button")

        # First attempt to get OCR text
        text = _wait_for_ocr_text(driver)
        word_count = len(text.split())
        log.info(f"    OCR attempt 1: {len(text)} chars, {word_count} words")

        # If text is too short, re-click the OCR button up to 4 more times
        for retry in range(4):
            if len(text) >= 2000:
                break
            log.info(f"    OCR text too short ({len(text)} chars). Re-clicking OCR button (retry {retry + 1}/4)...")
            _click_ocr_button(driver)
            new_text = _wait_for_ocr_text(driver)
            new_word_count = len(new_text.split())
            log.info(f"    OCR attempt {retry + 2}: {len(new_text)} chars, {new_word_count} words")
            if len(new_text) > len(text):
                text = new_text

    except Exception as e:
        log.warning(f"    Error extracting OCR: {e}")

    return text.strip()


def get_clip_url(driver):
    """Get the clip URL from the current page."""
    url = driver.current_url
    if "/clip/" in url:
        return url
    # Look for clip link on the page
    try:
        links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/clip/']")
        for link in links:
            href = link.get_attribute("href")
            if href:
                return href
    except Exception:
        pass
    return url


# === ARTICLE EXTRACTION (Claude Haiku) ===

def extract_articles_with_ai(ocr_text, date_str, newspaper, page):
    """Use Claude Haiku to extract 'lake worth' articles from OCR text.

    Returns list of dicts with 'headline' and 'text' keys.
    """
    if not ocr_text or len(ocr_text) < 20:
        return []

    # Check if "lake worth" appears in the text (allow OCR artifacts like "Lake. Worth", "Lake- Worth")
    import re as _re
    if not _re.search(r'(?i)lake[\s.\-,;:]+worth', ocr_text):
        return []

    try:
        import anthropic
        client = anthropic.Anthropic()

        prompt = f"""Below is OCR text from a newspaper page ({newspaper}, {date_str}, page {page}).

Extract ALL articles/items that mention "Lake Worth" (the lake, dam, or community near Fort Worth, Texas).

For each article found, provide:
- headline: The headline or title (if visible)
- text: The complete article text
- photo_description: If there is ANY indication that a photo, illustration, picture, map, or drawing accompanies this article, describe what the image likely shows. Look for captions, "photo", "picture", "illustration", "view of", "scene at", "map", or descriptive text suggesting an image. Also note if the article text references a visual ("as shown above", "pictured here", etc.). Return a brief description like "Aerial view of Lake Worth dam construction" or "Portrait of Mayor Smith". If no photo indication, return empty string "".

Rules:
- Include the full text of each article, not a summary
- If the headline isn't clear, use the first meaningful phrase
- If "Lake Worth" appears ANYWHERE in an article, extract the ENTIRE article — even if Lake Worth is not the main topic. A city commission article that mentions Lake Worth once must be extracted in full.
- Include EVERY article, notice, classified, legal notice, or item that mentions Lake Worth in any way — even brief mentions, addresses, road references, event listings, or passing references
- Do NOT skip anything. If "Lake Worth" appears in it, extract it. Zero tolerance for omissions.
- Preserve the original text as closely as possible, BUT fix obvious OCR errors: broken words (e.g. "com- munity" → "community"), garbled letters (e.g. "tlie" → "the", "liave" → "have"), stray punctuation from scan noise, and clearly misspelled common words. Do NOT change period language, unusual proper nouns, or anything that might be intentional early-1900s spelling.
- This is OCR text — expect artifacts like "Lake. Worth", "Lake- Worth", "Lake Worth", "Iake Worth", "Lnke Worth" etc. These all refer to Lake Worth.

Return JSON array: [{{"headline": "...", "text": "...", "photo_description": ""}}]
If no Lake Worth articles found, return: []

OCR TEXT:
{ocr_text}"""

        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=4096,
            messages=[{"role": "user", "content": prompt}],
        )

        result_text = response.content[0].text.strip()

        # Parse JSON from response (handle markdown code blocks)
        if "```" in result_text:
            m = re.search(r'```(?:json)?\s*(.*?)```', result_text, re.DOTALL)
            if m:
                result_text = m.group(1).strip()

        articles = json.loads(result_text)
        if isinstance(articles, list):
            return articles
        return []

    except ImportError:
        log.error("    anthropic package not installed. Run: pip install anthropic")
        return []
    except json.JSONDecodeError as e:
        log.warning(f"    AI returned invalid JSON: {e}")
        return []
    except Exception as e:
        log.warning(f"    AI extraction error: {e}")
        return []


# === MAIN CLIPPING LOOP ===

def build_clipped_image_ids(conn):
    """Build a set of image IDs that are already clipped, for fast lookup."""
    ids = set()
    # From processed_pdfs where clipped=1 and URL exists
    rows = conn.execute(
        "SELECT url FROM processed_pdfs WHERE clipped = 1 AND url IS NOT NULL"
    ).fetchall()
    for row in rows:
        m = re.search(r'/image/(\d+)', row[0])
        if m:
            ids.add(m.group(1))
    # From articles table — these filenames were processed even if not in processed_pdfs
    art_files = conn.execute(
        "SELECT DISTINCT pdf_filename FROM articles"
    ).fetchall()
    # Store filenames too for a secondary check
    done_filenames = set(row[0] for row in art_files if row[0])
    # From processed_pdfs clipped entries
    pp_files = conn.execute(
        "SELECT pdf_filename FROM processed_pdfs WHERE clipped = 1"
    ).fetchall()
    for row in pp_files:
        if row[0]:
            done_filenames.add(row[0])
    log.info(f"  Pre-loaded {len(ids)} clipped image IDs, {len(done_filenames)} done filenames")
    return ids, done_filenames


def is_url_clipped(url, clipped_image_ids):
    """Check if a URL's image ID is in the pre-loaded clipped set."""
    m = re.search(r'/image/(\d+)', url)
    if not m:
        return False
    return m.group(1) in clipped_image_ids


def clip_page(driver, url, conn, clipped_image_ids=None, done_filenames=None):
    """Visit a page, clip it, extract OCR, return (pdf_filename, clip_url, ocr_text, articles)."""

    # Skip if URL already clipped — no need to navigate
    if clipped_image_ids and is_url_clipped(url, clipped_image_ids):
        log.info(f"    Skip (no nav): {url[:60]}")
        return "skipped"

    # Navigate to the page
    page_start_time = time.time()
    track_view()
    driver.get(url)
    time.sleep(3)

    title = driver.title or ""
    meta = parse_page_title(title, url)

    if not meta["date"]:
        log.warning(f"    Could not parse title: {title[:60]}")
        return None

    pdf_filename = meta["pdf_filename"]

    # Skip if already clipped (by filename — belt and suspenders)
    if not needs_clipping(conn, pdf_filename):
        log.info(f"    Already clipped: {pdf_filename}")
        return "skipped"

    log.info(f"    Page: {pdf_filename}")

    # Step 1: Zoom out
    zoom_out(driver)
    time.sleep(1)

    # Step 2: Click Clip button
    if not click_clip_button(driver):
        log.warning(f"    Could not find Clip button for {pdf_filename}")
        return None

    time.sleep(2)

    # Step 3: Drag corners to cover full page — retry clip button up to 5 times
    handle_ok = drag_clip_corners(driver)
    if not handle_ok:
        for retry in range(5):
            log.warning(f"    Clip handle retry {retry + 1}/5 — re-clicking Clip button...")
            time.sleep(2)
            click_clip_button(driver)
            time.sleep(2)
            if drag_clip_corners(driver):
                handle_ok = True
                log.info(f"    Clip handles found on retry {retry + 1}")
                break
    if not handle_ok:
        log.warning(f"    Could not drag clip corners for {pdf_filename} after 5 retries. Skipping.")
        return None


    # Step 4: Save
    if not click_save_button(driver):
        log.warning(f"    Could not find Save button for {pdf_filename}")
        return None

    # Check for throttle message
    try:
        page_text = driver.execute_script("return document.body.innerText || '';")
        if "unable to create your clipping" in page_text.lower():
            log.warning("    THROTTLED: 'unable to create your clipping' detected. Stopping.")
            driver.execute_script("""
                var d = document.createElement('div');
                d.style.cssText = 'position:fixed;top:0;left:0;right:0;padding:20px;background:red;color:white;font-size:24px;font-weight:bold;z-index:999999;text-align:center';
                d.textContent = 'THROTTLED — Unable to create clipping. Account limit reached.';
                document.body.appendChild(d);
            """)
            return "stop"
    except Exception:
        pass

    # Step 5: Navigate to clip page
    if not navigate_to_clip_page(driver):
        log.warning(f"    Could not navigate to clip page for {pdf_filename}")
        return None

    time.sleep(2)

    # Step 6: Check clip image size — if too small, cursor was moved during clipping. Re-clip.
    clip_url = get_clip_url(driver)
    track_view()  # Count the clip view page too
    try:
        clip_img = driver.find_element(By.CSS_SELECTOR, "img[src*='clip'], img[src*='clipping'], img.article-image, main img")
        img_width = clip_img.get_attribute("naturalWidth") or clip_img.get_attribute("width")
        img_height = clip_img.get_attribute("naturalHeight") or clip_img.get_attribute("height")
        img_width = int(img_width or 0)
        img_height = int(img_height or 0)
        log.info(f"    Clip image size: {img_width}x{img_height}")
        if img_width > 0 and img_height > 0 and (img_width < 750 or img_height < 800):
            log.warning(f"    Clip too small ({img_width}x{img_height}). Re-clipping...")
            track_view()
            driver.get(url)
            time.sleep(3)
            zoom_out(driver)
            time.sleep(1)
            if click_clip_button(driver):
                time.sleep(2)
                if drag_clip_corners(driver):
                    if click_save_button(driver):
                        if navigate_to_clip_page(driver):
                            time.sleep(2)
                            clip_url = get_clip_url(driver)
                            track_view()
                            # Verify re-clip size
                            try:
                                clip_img2 = driver.find_element(By.CSS_SELECTOR, "img[src*='clip'], img[src*='clipping'], img.article-image, main img")
                                w2 = int(clip_img2.get_attribute("naturalWidth") or clip_img2.get_attribute("width") or 0)
                                h2 = int(clip_img2.get_attribute("naturalHeight") or clip_img2.get_attribute("height") or 0)
                                log.info(f"    Re-clip image size: {w2}x{h2}")
                            except Exception:
                                pass
    except Exception as e:
        log.info(f"    Could not check clip size: {e}")

    # Step 7: Extract OCR text (last step — after size is verified)
    ocr_text = extract_ocr_text(driver)
    log.info(f"    OCR text: {len(ocr_text)} chars, clip: {clip_url[:60]}...")

    # Save to DB
    save_clip_data(conn, pdf_filename, url, clip_url, ocr_text)

    # Extract articles with AI
    articles = extract_articles_with_ai(
        ocr_text, meta["date"], meta["newspaper"], meta["page"]
    )

    if articles:
        count = save_articles(conn, pdf_filename, articles, SEARCH_TERM, clip_url=clip_url)
        log.info(f"    Found {count} articles")
    else:
        log.info(f"    No Lake Worth articles found in OCR")

    elapsed = time.time() - page_start_time
    log.info(f"    Page: {pdf_filename} — {elapsed:.1f}s ({len(articles)} articles)")

    return {
        "pdf_filename": pdf_filename,
        "clip_url": clip_url,
        "ocr_len": len(ocr_text),
        "articles": len(articles),
        "date": meta["date"],
    }


def main(max_pages=0):
    conn = get_db()
    ensure_columns(conn)

    unclipped = conn.execute(
        "SELECT COUNT(*) as c FROM processed_pdfs WHERE clipped = 0 AND search_term = ?",
        (SEARCH_TERM,)
    ).fetchone()["c"]

    start_date = get_start_date(conn)
    clipped_image_ids, done_filenames = build_clipped_image_ids(conn)

    log.info("=" * 60)
    log.info("Full-Page Clipper + Article Extractor")
    log.info("=" * 60)
    log.info(f"  Unclipped entries: {unclipped}")
    log.info(f"  Starting from: {start_date}")
    log.info(f"  Max pages: {max_pages or 'unlimited'}")
    log.info(f"  Throttle: {VIEWS_PER_HOUR} views/hour")
    log.info(f"  Log: {log_filename}")

    clipped = 0
    skipped = 0
    errors = 0
    total_articles = 0
    driver = None
    keep_browser_open = False

    try:
        driver = setup_driver()

        current_date = start_date

        while current_date <= DATE_END:
            if keep_browser_open:
                break
            if max_pages and clipped >= max_pages:
                log.info(f"  Reached max_pages limit ({max_pages})")
                break

            # Build search URL — full range from current position to end
            window_end_dt = datetime.strptime(DATE_END, "%Y-%m-%d")
            window_end = DATE_END

            encoded_term = SEARCH_TERM.replace(" ", "+")
            search_url = (
                "https://star-telegram.newspapers.com/search/results/"
                f"?date-end={window_end}&date-start={current_date}"
                f"&keyword=%22{encoded_term}%22"
                "&sort=paper-date-asc"
            )

            log.info(f"\n>>> Searching {current_date} to {window_end}")
            track_view()
            driver.get(search_url)
            time.sleep(4)

            # Collect first batch of result URLs
            page_urls = collect_search_results(driver)
            log.info(f"  Found {len(page_urls)} results")

            if not page_urls:
                # No results — jump ahead by 6 months instead of 30 days
                jump = datetime.strptime(current_date, "%Y-%m-%d") + timedelta(days=180)
                current_date = jump.strftime("%Y-%m-%d")
                save_start_date(conn, current_date)
                log.info(f"  No results, jumping ahead to {current_date}")
                continue

            window_done = False

            seen_urls = set()
            batch_clips = 0

            while True:
                if max_pages and clipped >= max_pages:
                    break

                # Filter batch: skip seen and already-done results (no navigation needed)
                new_results = []
                for r in page_urls:
                    url = r["url"]
                    if url in seen_urls:
                        continue
                    seen_urls.add(url)
                    # Skip by filename (parsed from search result text)
                    if r["pdf_filename"] in done_filenames:
                        # Backfill the page URL even though we're skipping
                        backfill_page_url(conn, r["pdf_filename"], url)
                        skipped += 1
                        continue
                    # Skip by image ID
                    if is_url_clipped(url, clipped_image_ids):
                        backfill_page_url(conn, r["pdf_filename"], url)
                        skipped += 1
                        continue
                    new_results.append(r)
                if new_results:
                    log.info(f"  {len(new_results)} to clip this batch (skipped {skipped})")
                if not new_results:
                    # No new results — click Show More repeatedly to find more
                    found_new = False
                    while click_show_more(driver):
                        track_view()
                        time.sleep(3)
                        page_urls = collect_search_results(driver)
                        new_results = []
                        for r in page_urls:
                            url = r["url"]
                            if url in seen_urls:
                                continue
                            seen_urls.add(url)
                            if r["pdf_filename"] in done_filenames:
                                backfill_page_url(conn, r["pdf_filename"], url)
                                skipped += 1
                                continue
                            if is_url_clipped(url, clipped_image_ids):
                                backfill_page_url(conn, r["pdf_filename"], url)
                                skipped += 1
                                continue
                            new_results.append(r)
                        if new_results:
                            log.info(f"  Show More: {len(new_results)} to clip")
                            found_new = True
                            break
                    if not found_new:
                        # All results exhausted — advance date past the last result we saw
                        if page_urls:
                            last_date = max(r["date"] for r in page_urls if r["date"])
                            if last_date and last_date >= current_date:
                                next_date = (datetime.strptime(last_date, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
                                log.info(f"  All results done through {last_date}. Advancing to {next_date}")
                                current_date = next_date
                                save_start_date(conn, current_date)
                            else:
                                log.info(f"  No more results. Done.")
                        else:
                            log.info(f"  No more results. Done.")
                        break

                for r in new_results:
                    url = r["url"]
                    if max_pages and clipped >= max_pages:
                        break

                    try:
                        result = clip_page(driver, url, conn, clipped_image_ids, done_filenames)
                        if result == "skipped":
                            skipped += 1
                        elif result == "throttled":
                            log.info("  Resuming after throttle wait...")
                        elif result == "stop":
                            log.warning("  Failure — browser left open for inspection. Exiting.")
                            keep_browser_open = True
                            break
                        elif result is None:
                            errors += 1
                        else:
                            clipped += 1
                            batch_clips += 1
                            total_articles += result.get("articles", 0)
                            # Add to clipped sets so future batches skip this URL
                            m = re.search(r'/image/(\d+)', url)
                            if m:
                                clipped_image_ids.add(m.group(1))
                            if result.get("pdf_filename"):
                                done_filenames.add(result["pdf_filename"])
                            # Increment date counter to this page's date
                            if result.get("date"):
                                save_start_date(conn, result["date"])
                            log.info(f"  Progress: {clipped} clipped, {total_articles} articles, {errors} errors")
                    except (WebDriverException, InvalidSessionIdException) as e:
                        log.error(f"    Session error: {e}")
                        errors += 1
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        log.info("    Restarting browser session...")
                        driver = setup_driver()
                        log.info("    Session recovered.")
                    except Exception as e:
                        log.error(f"    Clip error: {e}")
                        errors += 1

                    if keep_browser_open:
                        break

                    # Every 100 clips, restart browser and rebuild search from current date
                    if batch_clips >= 100:
                        log.info(f"  === 100-clip checkpoint. Restarting browser. ===")
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = setup_driver()
                        # Rebuild search URL from saved date counter
                        current_date = get_start_date(conn)
                        encoded_term = SEARCH_TERM.replace(" ", "+")
                        search_url = (
                            "https://star-telegram.newspapers.com/search/results/"
                            f"?date-end={DATE_END}&date-start={current_date}"
                            f"&keyword=%22{encoded_term}%22"
                            "&sort=paper-date-asc"
                        )
                        log.info(f"  Rebuilt search from {current_date}")
                        track_view()
                        driver.get(search_url)
                        time.sleep(4)
                        page_urls = collect_search_results(driver)
                        seen_urls.clear()
                        batch_clips = 0
                        break  # break out of for loop, continue while loop

                if keep_browser_open:
                    break

                # Return to search page and click Show More for next batch
                track_view()
                driver.get(search_url)
                time.sleep(4)
                page_urls = collect_search_results(driver)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    except SystemExit:
        pass  # clean exit with browser left open
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if driver and not keep_browser_open:
            try:
                driver.quit()
            except Exception:
                pass

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Pages clipped: {clipped}")
    log.info(f"  Articles found: {total_articles}")
    log.info(f"  Skipped: {skipped}")
    log.info(f"  Errors: {errors}")
    log.info("=" * 60)
    conn.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else 0
    main(max_pages=limit)
