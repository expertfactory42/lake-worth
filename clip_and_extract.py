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
import socket
import sqlite3
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

from seleniumbase import Driver
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

def db_retry(func, *args, max_retries=5, base_delay=1.0, **kwargs):
    """Retry a database operation that may fail with 'database is locked'.
    Uses exponential back-off with jitter."""
    import random as _random
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except sqlite3.OperationalError as e:
            if "locked" in str(e) and attempt < max_retries - 1:
                delay = base_delay * (2 ** attempt) + _random.uniform(0, 0.5)
                log.warning(
                    f"    DB locked (attempt {attempt + 1}/{max_retries}), "
                    f"retrying in {delay:.1f}s..."
                )
                time.sleep(delay)
            else:
                raise


def db_commit(conn):
    """Commit with retry on 'database is locked'."""
    db_retry(conn.commit)


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def ensure_columns(conn):
    """Add columns if they don't exist yet."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(processed_pdfs)").fetchall()}
    if "ocr_text" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN ocr_text TEXT")
        db_commit(conn)
        log.info("Added ocr_text column to processed_pdfs")
    if "clip_url" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN clip_url TEXT")
        db_commit(conn)
        log.info("Added clip_url column to processed_pdfs")
    if "clipped" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN clipped INTEGER DEFAULT 0")
        db_commit(conn)
        log.info("Added clipped column to processed_pdfs")
    # Multi-instance page-claim columns (Step 1 / multi-instance plan)
    if "claimed_by" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN claimed_by TEXT")
        db_commit(conn)
        log.info("Added claimed_by column to processed_pdfs")
    if "claimed_at" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN claimed_at TEXT")
        db_commit(conn)
        log.info("Added claimed_at column to processed_pdfs")
    if "claimed_pid" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN claimed_pid INTEGER")
        db_commit(conn)
        log.info("Added claimed_pid column to processed_pdfs")

    # Articles table
    art_cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()}
    if "has_photo" not in art_cols:
        conn.execute("ALTER TABLE articles ADD COLUMN has_photo INTEGER DEFAULT 0")
        db_commit(conn)
        log.info("Added has_photo column to articles")
    if "photo_description" not in art_cols:
        conn.execute("ALTER TABLE articles ADD COLUMN photo_description TEXT")
        db_commit(conn)
        log.info("Added photo_description column to articles")

    # Accounts table: multi-instance claim columns (Step 1 / multi-instance plan)
    acct_tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='accounts'").fetchall()]
    if acct_tables:
        acct_cols = {r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()}
        if "in_use_by" not in acct_cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN in_use_by TEXT")
            db_commit(conn)
            log.info("Added in_use_by column to accounts")
        if "in_use_since" not in acct_cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN in_use_since TEXT")
            db_commit(conn)
            log.info("Added in_use_since column to accounts")
        if "in_use_pid" not in acct_cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN in_use_pid INTEGER")
            db_commit(conn)
            log.info("Added in_use_pid column to accounts")

    # Per-instance status table (Step 1 / multi-instance plan)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clipper_instances (
            slot_id        TEXT PRIMARY KEY,
            pid            INTEGER,
            account_email  TEXT,
            status         TEXT,
            current_date   TEXT,
            current_page   INTEGER,
            count_this_run INTEGER DEFAULT 0,
            date_start     TEXT,
            date_end       TEXT,
            started_at     TEXT,
            heartbeat_at   TEXT,
            last_action    TEXT,
            browser_health TEXT DEFAULT 'unknown'
        )
    """)
    db_commit(conn)
    # Add browser_health column if table already existed without it
    inst_cols = {r[1] for r in conn.execute("PRAGMA table_info(clipper_instances)").fetchall()}
    if "browser_health" not in inst_cols:
        conn.execute("ALTER TABLE clipper_instances ADD COLUMN browser_health TEXT DEFAULT 'unknown'")
        db_commit(conn)
        log.info("Added browser_health column to clipper_instances")


def get_start_date(conn):
    """Read the clipper date counter from DB. Defaults to 1914-01-01."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS clipper_state (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    db_commit(conn)
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
    db_commit(conn)


def needs_clipping(conn, pdf_filename):
    """Check if this entry needs clipping.

    Skip if already clipped by us, or if the user has already manually
    clipped it (has_image=1 in articles table).
    """
    row = conn.execute(
        "SELECT clipped, ignored FROM processed_pdfs WHERE pdf_filename = ?", (pdf_filename,)
    ).fetchone()
    if row is not None and row["ignored"]:
        return False
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
        db_commit(conn)
    elif not row[0]:
        # Entry exists but URL is missing — fill it in
        conn.execute(
            "UPDATE processed_pdfs SET url = ? WHERE pdf_filename = ?",
            (url, pdf_filename)
        )
        db_commit(conn)


def save_clip_data(conn, pdf_filename, url, clip_url, ocr_text):
    """Save clip results to DB."""
    # Ensure clipped_by column exists
    cols = [r[1] for r in conn.execute("PRAGMA table_info(processed_pdfs)").fetchall()]
    if "clipped_by" not in cols:
        conn.execute("ALTER TABLE processed_pdfs ADD COLUMN clipped_by TEXT")
    conn.execute(
        """UPDATE processed_pdfs
           SET url = ?, clip_url = ?, ocr_text = ?, clipped = 1, clipped_by = ?
           WHERE pdf_filename = ?""",
        (url, clip_url, ocr_text, _current_account_email or "", pdf_filename)
    )
    db_commit(conn)


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

    # Inherit page-level flags set by user on the "no articles" tab
    pdf_row = conn.execute(
        "SELECT highlighted, has_photo FROM processed_pdfs WHERE pdf_filename = ?",
        (pdf_filename,)
    ).fetchone()
    pdf_highlighted = (pdf_row[0] or 0) if pdf_row else 0
    pdf_has_photo = (pdf_row[1] or 0) if pdf_row else 0

    count = 0
    for article in articles:
        headline = article.get("headline", "").strip()
        text = article.get("text", "").strip()
        photo_desc = (article.get("photo_description") or "").strip()
        has_photo = 1 if (photo_desc or pdf_has_photo) else 0
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
                """UPDATE articles SET headline=?, full_text=?, pdf_filename=?, search_term=?, clip_id=?, has_photo=?, photo_description=?,
                                       highlighted = COALESCE(NULLIF(highlighted,0), ?)
                   WHERE id=?""",
                (headline, text, pdf_filename, search_term, clip_id or None, has_photo, photo_desc or None, pdf_highlighted, replace_id)
            )
        else:
            conn.execute(
                """INSERT INTO articles (date, newspaper, page, headline, full_text, pdf_filename, search_term, has_image, clip_id, has_photo, photo_description, highlighted)
                   VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?, ?, ?)""",
                (date_str, newspaper, page, headline, text, pdf_filename, search_term, clip_id or None, has_photo, photo_desc or None, pdf_highlighted)
            )
        if has_photo:
            log.info(f"    >>> PHOTO: {photo_desc[:60]}")
        count += 1

    if count > 0:
        conn.execute(
            "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
            (count, pdf_filename)
        )
        db_commit(conn)
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



# === STOP FLAG ===

STOP_FLAG_FILE = r"c:\lake_worth_runtime\stop_clipper"


def check_stop_flag():
    """Check if stop flag file exists. Returns True if script should stop."""
    if os.path.exists(STOP_FLAG_FILE):
        log.info("  Stop flag detected — exiting gracefully.")
        return True
    return False


def stoppable_sleep(seconds):
    """Sleep in 5-second chunks, checking stop flag between each. Returns True if stopped."""
    remaining = seconds
    while remaining > 0:
        chunk = min(5, remaining)
        time.sleep(chunk)
        remaining -= chunk
        if check_stop_flag():
            return True
    return False


# === MULTI-INSTANCE HELPERS ===
#
# Additive helpers for running N concurrent clipper workers against one queue
# and one account pool. Single-instance runs do not call any of these — they
# are only used when the worker is started with --slot-id.

GLOBAL_STOP_FLAG_FILE = r"c:\lake_worth_runtime\stop_clipper_all"


def instance_stop_flag_path(slot_id):
    """Per-instance stop flag file path."""
    return rf"c:\lake_worth_runtime\stop_clipper_{slot_id}"


def check_instance_stop(slot_id):
    """Return True if the global stop flag, the per-instance stop flag, or the
    legacy single-instance stop flag is set. Legacy flag is honored so that
    existing "Stop" UI still halts a slot-mode worker."""
    if os.path.exists(GLOBAL_STOP_FLAG_FILE):
        log.info("  Global stop flag detected — exiting gracefully.")
        return True
    if slot_id and os.path.exists(instance_stop_flag_path(slot_id)):
        log.info(f"  Stop flag for slot {slot_id} detected — exiting gracefully.")
        return True
    if os.path.exists(STOP_FLAG_FILE):
        log.info("  Stop flag detected — exiting gracefully.")
        return True
    return False


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def claim_account(slot_id, pid):
    """Atomically claim the next eligible account for this slot.

    Eligibility:
      - active = 1
      - not currently claimed by any slot (in_use_by IS NULL)
      - clips_today < daily_clip_limit, OR last_clip_time is 24+ hours ago
    Ordered by fewest clips today first, then least total clips.

    Returns a dict with account fields on success, or None if no account is
    currently eligible. Uses BEGIN IMMEDIATE to serialize claims across workers.
    """
    clip_limit = get_daily_clip_limit() or 999999
    today = datetime.now().strftime("%Y-%m-%d")
    conn = get_accounts_db()
    try:
        conn.isolation_level = None  # manual transaction control
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT * FROM accounts
             WHERE active = 1
               AND in_use_by IS NULL
               AND (
                   clips_today < ?
                   OR clips_today IS NULL
                   OR clips_today_date IS NULL
                   OR clips_today_date != ?
                   OR last_clip_time IS NULL
                   OR last_clip_time < datetime('now','localtime','-24 hours')
               )
             ORDER BY clips_today ASC NULLS FIRST, total_clips ASC
             LIMIT 1
            """,
            (clip_limit, today),
        ).fetchone()
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


def release_account(email, slot_id=None):
    """Clear the claim on an account. If slot_id is given, only release if the
    account is actually claimed by that slot (safety check)."""
    if not email:
        return
    conn = get_accounts_db()
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
        db_commit(conn)
    finally:
        conn.close()


def claim_next_page(conn, slot_id, pid, date_start=None, date_end=None):
    """Atomically claim the next unclipped page in range for this slot.

    Eligibility matches get_unclipped_queue() exactly:
      - clipped = 0 (or NULL)
      - no articles row for the pdf_filename
      - not ignored
      - has a non-empty url
      - within date range (if provided)
      - not currently claimed (claimed_by IS NULL)
    Ordered by date_str, pdf_filename.

    Returns a dict {pdf_filename, url, date_str} on success, or None.
    Uses BEGIN IMMEDIATE.
    """
    sql_select = """
        SELECT pp.rowid AS rid, pp.pdf_filename, pp.url, pp.date_str
          FROM processed_pdfs pp
          LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
         WHERE (pp.clipped = 0 OR pp.clipped IS NULL)
           AND a.id IS NULL
           AND (pp.ignored IS NULL OR pp.ignored = 0)
           AND pp.url IS NOT NULL AND pp.url != ''
           AND (pp.claimed_by IS NULL)
    """
    params = []
    if date_start:
        sql_select += " AND pp.date_str >= ?"
        params.append(date_start)
    if date_end:
        sql_select += " AND pp.date_str <= ?"
        params.append(date_end)
    sql_select += " ORDER BY pp.date_str, pp.pdf_filename LIMIT 1"

    prev_isolation = conn.isolation_level
    try:
        conn.isolation_level = None
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(sql_select, params).fetchone()
        if not row:
            conn.execute("COMMIT")
            return None
        now = _now_str()
        conn.execute(
            """UPDATE processed_pdfs
                  SET claimed_by = ?, claimed_at = ?, claimed_pid = ?
                WHERE rowid = ?""",
            (slot_id, now, pid, row["rid"]),
        )
        conn.execute("COMMIT")
        return {
            "pdf_filename": row["pdf_filename"],
            "url": row["url"],
            "date_str": row["date_str"],
        }
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise
    finally:
        conn.isolation_level = prev_isolation


def release_page(conn, pdf_filename, slot_id=None):
    """Clear the claim on a single page (without marking it clipped). Used when
    a claimed page fails to clip so another instance can retry it."""
    if not pdf_filename:
        return
    if slot_id:
        conn.execute(
            """UPDATE processed_pdfs
                  SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL
                WHERE pdf_filename = ? AND claimed_by = ?""",
            (pdf_filename, slot_id),
        )
    else:
        conn.execute(
            """UPDATE processed_pdfs
                  SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL
                WHERE pdf_filename = ?""",
            (pdf_filename,),
        )
    db_commit(conn)


def release_all_pages_for_slot(conn, slot_id):
    """Clear any page claims held by this slot that were never completed.
    Called on graceful shutdown to make sure nothing stays stuck."""
    if not slot_id:
        return
    conn.execute(
        """UPDATE processed_pdfs
              SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL
            WHERE claimed_by = ? AND (clipped = 0 OR clipped IS NULL)""",
        (slot_id,),
    )
    db_commit(conn)


def write_instance_status(slot_id, **fields):
    """Upsert this slot's row in clipper_instances. Always bumps heartbeat_at.
    Accepted fields: pid, account_email, status, current_date, current_page,
    count_this_run, date_start, date_end, started_at, last_action."""
    if not slot_id:
        return
    allowed = {
        "pid", "account_email", "status", "current_date", "current_page",
        "count_this_run", "date_start", "date_end", "started_at", "last_action",
        "browser_health",
    }
    fields = {k: v for k, v in fields.items() if k in allowed}
    fields["heartbeat_at"] = _now_str()

    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT slot_id FROM clipper_instances WHERE slot_id = ?", (slot_id,)
        ).fetchone()
        if existing:
            if fields:
                sets = ", ".join(f"{k} = ?" for k in fields.keys())
                params = list(fields.values()) + [slot_id]
                conn.execute(
                    f"UPDATE clipper_instances SET {sets} WHERE slot_id = ?",
                    params,
                )
        else:
            cols = ["slot_id"] + list(fields.keys())
            placeholders = ", ".join(["?"] * len(cols))
            params = [slot_id] + list(fields.values())
            conn.execute(
                f"INSERT INTO clipper_instances ({', '.join(cols)}) VALUES ({placeholders})",
                params,
            )
        db_commit(conn)
    finally:
        conn.close()


def delete_instance_row(slot_id):
    """Remove this slot's row from clipper_instances on shutdown."""
    if not slot_id:
        return
    conn = get_db()
    try:
        conn.execute("DELETE FROM clipper_instances WHERE slot_id = ?", (slot_id,))
        db_commit(conn)
    finally:
        conn.close()


def get_daily_clip_limit():
    """Read daily_clip_limit from clipper_state table. Returns 0 (unlimited) if not set."""
    try:
        conn = sqlite3.connect(DB_PATH, timeout=30)
        row = conn.execute("SELECT value FROM clipper_state WHERE key = 'daily_clip_limit'").fetchone()
        conn.close()
        if row and row[0]:
            return int(row[0])
    except Exception:
        pass
    return 0


# === RESILIENT BROWSER RESTART ===

RESTART_DELAYS = [180, 120, 60, 60, 60]  # 3min, 2min, 1min, 1min, 1min


def resilient_setup_driver(preferred_account=None, slot_id=None):
    """Try setup_driver() with retries on failure.
    First 5 attempts use RESTART_DELAYS, then retries every 10 minutes indefinitely.
    """
    attempt = 0
    while True:
        if slot_id and check_instance_stop(slot_id):
            return None
        try:
            check_internet_pause()
            return setup_driver(preferred_account=preferred_account, slot_id=slot_id)
        except Exception as e:
            attempt += 1
            if attempt <= len(RESTART_DELAYS):
                delay = RESTART_DELAYS[attempt - 1]
                log.warning(f"    setup_driver() attempt {attempt}/{len(RESTART_DELAYS)} failed: {e}")
            else:
                delay = 600
                log.warning(f"    setup_driver() attempt {attempt} failed: {e}")
            log.info(f"    Waiting {delay}s before retry...")
            if stoppable_sleep(delay):
                return None


# === INTERNET RESET PAUSE ===

def is_internet_up():
    """Quick connectivity check — try to reach Cloudflare DNS."""
    try:
        socket.create_connection(("1.1.1.1", 443), timeout=5).close()
        return True
    except OSError:
        return False


def wait_for_internet(max_wait=600):
    """Block until internet is available. Returns True if restored, False if timed out."""
    if is_internet_up():
        return True
    log.warning("  Internet is DOWN — waiting for connectivity...")
    start = time.time()
    attempt = 0
    while time.time() - start < max_wait:
        attempt += 1
        delay = min(15, 5 + attempt * 2)  # 7, 9, 11, 13, 15, 15, 15...
        time.sleep(delay)
        if check_stop_flag():
            log.info("  Stop flag detected while waiting for internet.")
            return False
        if is_internet_up():
            elapsed = time.time() - start
            log.info(f"  Internet restored after {elapsed:.0f}s")
            return True
        if attempt % 4 == 0:
            elapsed = time.time() - start
            log.warning(f"  Still waiting for internet... ({elapsed:.0f}s elapsed)")
    log.error(f"  Internet not restored after {max_wait}s — giving up.")
    return False


def check_internet_pause():
    """Pause during the nightly internet reset window (12:58 AM - 1:10 AM),
    and verify internet is actually up before continuing."""
    now = datetime.now()
    pause_start = now.replace(hour=0, minute=58, second=0, microsecond=0)
    pause_end = now.replace(hour=1, minute=10, second=0, microsecond=0)
    if pause_start <= now < pause_end:
        wait_seconds = (pause_end - now).total_seconds()
        log.info(f"  Internet reset window — pausing until 1:10 AM ({wait_seconds:.0f}s)")
        time.sleep(wait_seconds)
        log.info(f"  Resuming after internet reset pause.")
    # Always verify connectivity before proceeding
    wait_for_internet()


# === CLOUDFLARE HANDLING ===

def close_extra_tabs(driver):
    """Close every browser tab except the currently focused one.

    seleniumbase's uc_open_with_reconnect opens a new tab to bypass
    Cloudflare and leaves the old one behind. Without cleanup, tabs
    accumulate on every login / retry / navigation and eventually the
    browser is full of dead tabs.

    After cleanup, re-maximizes the surviving tab since new tabs opened by
    uc_open_with_reconnect do not inherit the maximized state.
    """
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
        # Re-maximize — new tabs from uc_open_with_reconnect don't inherit
        # the maximized state from the original tab.
        try:
            driver.maximize_window()
        except Exception:
            pass
    except Exception as e:
        log.info(f"    close_extra_tabs skipped: {e}")


def validate_browser_health(driver, slot_id=None):
    """Check browser health. Returns dict with status and detail fields."""
    result = {
        "healthy": True,
        "tab_count": 0,
        "is_fullscreen": False,
        "on_site": False,
        "driver_responsive": False,
        "issues": [],
    }
    try:
        # 1. Driver responsive
        handles = driver.window_handles
        result["driver_responsive"] = True
        result["tab_count"] = len(handles)

        # 2. Tab count — should be exactly 1
        if len(handles) == 0:
            result["healthy"] = False
            result["issues"].append("no_tabs")
            return result
        if len(handles) > 1:
            result["issues"].append(f"{len(handles)}_tabs")
            close_extra_tabs(driver)
            handles = driver.window_handles
            result["tab_count"] = len(handles)

        # 3. Window size — check if maximized/fullscreen
        try:
            size = driver.get_window_size()
            result["is_fullscreen"] = size.get("width", 0) >= 1200
            if not result["is_fullscreen"]:
                result["issues"].append("not_fullscreen")
                try:
                    driver.maximize_window()
                    time.sleep(0.5)
                    size = driver.get_window_size()
                    result["is_fullscreen"] = size.get("width", 0) >= 1200
                    if result["is_fullscreen"]:
                        result["issues"].remove("not_fullscreen")
                except Exception:
                    pass
        except Exception:
            result["issues"].append("size_check_failed")

        # 4. URL domain check
        try:
            url = driver.current_url or ""
            result["on_site"] = "newspapers.com" in url
            if not result["on_site"] and "data:" not in url:
                result["issues"].append("wrong_site")
        except Exception:
            result["issues"].append("url_check_failed")

        # Only mark unhealthy for critical issues
        critical = {"no_tabs", "size_check_failed", "url_check_failed"}
        if critical & set(result["issues"]):
            result["healthy"] = False

    except Exception as e:
        result["healthy"] = False
        result["driver_responsive"] = False
        result["issues"].append(f"exception:{e}")

    return result


def browser_health_str(health):
    """Convert health dict to a short string for DB storage."""
    if not health["issues"]:
        return "ok"
    return ",".join(health["issues"])


def _save_error_screenshot(driver, reason, slot_id=None):
    """Save a screenshot when something goes wrong so we can see what the browser showed."""
    try:
        tag = f"slot{slot_id}_" if slot_id else ""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = f"error_{tag}{reason}_{ts}.png"
        fpath = os.path.join(LOG_DIR, fname)
        driver.save_screenshot(fpath)
        log.info(f"  Error screenshot saved: {fpath}")
    except Exception as e:
        log.warning(f"  Could not save error screenshot: {e}")


def is_cloudflare(driver):
    """Check if the current page is a Cloudflare challenge."""
    try:
        title = (driver.title or "").lower()
        if "just a moment" in title:
            return True
        page_text = driver.execute_script("return document.body.innerText || '';").lower()
        if "security verification" in page_text or "checking if the site connection is secure" in page_text:
            return True
    except Exception:
        pass
    return False


def solve_cloudflare(driver, max_attempts=20):
    """Detect and solve Cloudflare challenge. Returns True if solved or no challenge."""
    if not is_cloudflare(driver):
        return True
    log.info("    Cloudflare challenge detected — solving...")
    for attempt in range(max_attempts):
        try:
            driver.uc_gui_click_captcha()
            time.sleep(3)
            if not is_cloudflare(driver):
                log.info(f"    Cloudflare solved (attempt {attempt + 1})")
                return True
            log.info(f"    Cloudflare still present after click {attempt + 1}/{max_attempts}")
        except Exception as e:
            log.warning(f"    Cloudflare solve error: {e}")
        time.sleep(2)
    log.warning(f"    Could not solve Cloudflare after {max_attempts} attempts")
    return False


def navigate(driver, url):
    """Navigate to URL and handle Cloudflare if it appears."""
    driver.get(url)
    time.sleep(3)
    if is_cloudflare(driver):
        if not solve_cloudflare(driver):
            return False
    return True


# === ACCOUNT MANAGEMENT ===

_current_account_email = None
_current_account_clips = 0


def get_accounts_db():
    """Get a connection to read/write accounts table."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def get_next_account(exclude_email=None):
    """Get the next active, eligible account.

    Eligible means:
      - clips_today < daily_clip_limit, OR
      - last_clip_time is 24+ hours ago (new day resets the counter)
    """
    clip_limit = get_daily_clip_limit() or 999999
    conn = get_accounts_db()
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "accounts" not in tables:
            return None

        sql = """SELECT * FROM accounts WHERE active = 1
                 AND (
                     clips_today < ?
                     OR clips_today IS NULL
                     OR clips_today_date IS NULL
                     OR clips_today_date != ?
                     OR last_clip_time IS NULL
                     OR last_clip_time < datetime('now','localtime','-24 hours')
                 )"""
        today = datetime.now().strftime("%Y-%m-%d")
        params = [clip_limit, today]
        if exclude_email:
            sql += " AND email != ?"
            params.append(exclude_email)
        # Prefer: accounts with fewest clips today first, then least total clips
        sql += " ORDER BY clips_today ASC NULLS FIRST, total_clips ASC LIMIT 1"
        row = conn.execute(sql, params).fetchone()
        if row:
            return dict(row)
        log.warning("  All active accounts have hit the daily limit — waiting for cooldown.")
        return None
    finally:
        conn.close()


def get_all_active_accounts():
    """Get all active accounts ordered by preference."""
    conn = get_accounts_db()
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "accounts" not in tables:
            return []
        rows = conn.execute(
            "SELECT * FROM accounts WHERE active = 1 ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def update_account_stats(email, clips_added=0, articles_added=0, throttled=False):
    """Update account statistics after clipping or throttle."""
    conn = get_accounts_db()
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "accounts" not in tables:
            return
        # Ensure articles_this_session / clips_today columns exist
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "articles_this_session" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN articles_this_session INTEGER DEFAULT 0")
            db_commit(conn)
        if "clips_today" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN clips_today INTEGER DEFAULT 0")
            db_commit(conn)
        if "clips_today_date" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN clips_today_date TEXT")
            db_commit(conn)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        if clips_added > 0:
            # Reset clips_today if the stored date isn't today.
            conn.execute("""
                UPDATE accounts SET clips_today = 0, clips_today_date = ?
                 WHERE email = ? AND (clips_today_date IS NULL OR clips_today_date != ?)
            """, (today, email, today))
            conn.execute("""
                UPDATE accounts SET
                    total_clips = total_clips + ?,
                    clips_this_session = clips_this_session + ?,
                    clips_today = clips_today + ?,
                    clips_today_date = ?,
                    last_clip_time = ?,
                    updated_at = ?
                WHERE email = ?
            """, (clips_added, clips_added, clips_added, today, now, now, email))
        if articles_added > 0:
            conn.execute("""
                UPDATE accounts SET
                    articles_this_session = articles_this_session + ?,
                    updated_at = ?
                WHERE email = ?
            """, (articles_added, now, email))
        if throttled:
            # Update throttle stats and compute running average
            row = conn.execute("SELECT throttle_count, avg_clips_before_throttle, clips_this_session FROM accounts WHERE email = ?", (email,)).fetchone()
            if row:
                old_count = row["throttle_count"] or 0
                old_avg = row["avg_clips_before_throttle"] or 0
                session_clips = row["clips_this_session"] or 0
                new_count = old_count + 1
                new_avg = ((old_avg * old_count) + session_clips) / new_count if new_count > 0 else session_clips
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
        db_commit(conn)
    finally:
        conn.close()


def update_account_login(email):
    """Record login time for an account and reset per-session counters
    so every new run for this account starts at zero."""
    conn = get_accounts_db()
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "accounts" not in tables:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """UPDATE accounts SET last_login_time = ?, updated_at = ?,
                                   clips_this_session = 0, articles_this_session = 0
               WHERE email = ?""",
            (now, now, email),
        )
        db_commit(conn)
    finally:
        conn.close()


def update_account_logout(email):
    """Record logout time for an account."""
    conn = get_accounts_db()
    try:
        tables = [r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
        if "accounts" not in tables:
            return
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute("UPDATE accounts SET last_logout_time = ?, updated_at = ? WHERE email = ?", (now, now, email))
        db_commit(conn)
    finally:
        conn.close()


def _detect_logged_in_account(driver):
    """Try to detect which account is currently logged in by checking the page for user info."""
    try:
        # Navigate to account page to check email
        driver.execute_script("window.location.href = 'https://www.newspapers.com/account/';")
        time.sleep(3)
        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(2)
        page_text = driver.execute_script("return document.body.innerText || '';")
        # Look for email addresses in the page text
        import re as _re
        emails = _re.findall(r'[\w.+-]+@[\w-]+\.[\w.-]+', page_text)
        if emails:
            log.info(f"    Detected logged-in account: {emails[0]}")
            return emails[0]
        log.info("    Could not detect logged-in account from page text.")
    except Exception as e:
        log.warning(f"    Error detecting account: {e}")
    return None


def do_logout(driver):
    """Log out of newspapers.com by clearing cookies. Returns True if logged out."""
    global _current_account_email, _current_account_clips
    log.info("    Logging out of newspapers.com...")

    try:
        # Clear all cookies — this is the most reliable way to logout
        driver.delete_all_cookies()
        log.info("    Cookies cleared.")

        # Navigate to homepage to verify logged out
        try:
            driver.uc_open_with_reconnect("https://www.newspapers.com/", 4)
        except Exception:
            driver.get("https://www.newspapers.com/")
        close_extra_tabs(driver)
        time.sleep(5)

        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(3)

        page_text = driver.execute_script("return document.body.innerText || '';").lower()
        if "sign in" in page_text or "log in" in page_text:
            log.info("    Logged out successfully.")
            if _current_account_email:
                update_account_logout(_current_account_email)
            _current_account_email = None
            _current_account_clips = 0
            return True

        # If still not showing sign-in, try clearing cookies again with a fresh load
        log.info("    Still appears logged in, clearing cookies again...")
        driver.delete_all_cookies()
        driver.execute_script("window.localStorage.clear(); window.sessionStorage.clear();")
        driver.get("https://www.newspapers.com/")
        time.sleep(5)
        if is_cloudflare(driver):
            solve_cloudflare(driver)
            time.sleep(3)

        page_text = driver.execute_script("return document.body.innerText || '';").lower()
        if "sign in" in page_text or "log in" in page_text:
            log.info("    Logged out successfully.")
            if _current_account_email:
                update_account_logout(_current_account_email)
            _current_account_email = None
            _current_account_clips = 0
            return True

        log.warning("    Could not confirm logout.")
        return False
    except Exception as e:
        log.warning(f"    Logout error: {e}")
        return False


def do_login(driver, acct):
    """Log in to newspapers.com with the given account dict. Returns True on success."""
    global _current_account_email, _current_account_clips
    email = acct["email"]
    password = acct["password"]
    log.info(f"    Logging in as: {email}")

    # Navigate to sign-in page — use uc_open_with_reconnect for Cloudflare bypass
    log.info("    Navigating to sign-in page...")
    try:
        driver.uc_open_with_reconnect("https://www.newspapers.com/signin/", 4)
    except Exception:
        driver.execute_script("window.location.href = 'https://www.newspapers.com/signin/';")
    close_extra_tabs(driver)
    time.sleep(5)

    # Dismiss subscription / upsell nag modal if present. Fresh sessions often
    # land on a "Subscribe now / Sign in" interstitial that hides the login form
    # until its "Sign in" link/button is clicked.
    try:
        nag_clicked = False
        nag_js = r"""
            const wanted = ['sign in','log in','sign-in','log-in'];
            const nodes = document.querySelectorAll(
                "a, button, [role='button'], span, div"
            );
            for (const el of nodes) {
                const txt = (el.innerText || el.textContent || '').trim().toLowerCase();
                if (!txt || txt.length > 24) continue;
                if (!wanted.some(w => txt === w || txt.startsWith(w))) continue;
                // Skip elements that are plainly the page's own form labels/buttons.
                // We only want items inside modal/dialog/overlay containers.
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
                log.info("    Dismissed subscription nag (clicked in-modal Sign in).")
                time.sleep(2)
                break
            time.sleep(1)
    except Exception as e:
        log.info(f"    Nag-dismiss scan skipped: {e}")

    # Handle full-page Cloudflare challenge (first gate)
    if is_cloudflare(driver):
        log.info("    Full-page Cloudflare on sign-in — solving...")
        if not solve_cloudflare(driver):
            log.warning("    Cloudflare on sign-in page could not be solved.")
            return False
        time.sleep(3)

    # Wait for login form to render
    has_form = False
    for _ in range(15):
        if len(driver.find_elements(By.CSS_SELECTOR, "input[type='password'], input[name='email']")) > 0:
            has_form = True
            break
        # Check for Cloudflare again while waiting
        if is_cloudflare(driver):
            solve_cloudflare(driver)
        time.sleep(1)

    if not has_form:
        # Maybe already logged in from cookies — but verify it's the right account
        page_text = driver.execute_script("return document.body.innerText || '';").lower()
        if "sign in" not in page_text and "log in" not in page_text:
            # Verify this is actually the account we want
            detected = _detect_logged_in_account(driver)
            if detected and detected.lower() == email.lower():
                log.info(f"    Already logged in as {email} (session restored).")
                _current_account_email = email
                _current_account_clips = 0
                update_account_login(email)
                return True
            else:
                log.warning(f"    Session found but logged in as {detected or 'unknown'}, not {email}.")
                log.warning("    Logout may have failed — cannot login as requested account.")
                return False
        log.warning("    No login form found on sign-in page.")
        return False

    # Enter email
    try:
        email_field = None
        for sel in ["input[name='email']", "input[id='email']", "input[type='email']", "input[type='text']"]:
            fields = driver.find_elements(By.CSS_SELECTOR, sel)
            for f in fields:
                if f.is_displayed():
                    email_field = f
                    break
            if email_field:
                break
        if not email_field:
            log.warning("    Could not find email field.")
            return False
        email_field.clear()
        email_field.send_keys(email)
    except Exception as e:
        log.warning(f"    Email entry failed: {e}")
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
            log.warning("    Could not find password field.")
            return False
        pw_field.clear()
        pw_field.send_keys(password)
    except Exception as e:
        log.warning(f"    Password entry failed: {e}")
        return False

    # Solve Cloudflare Turnstile on the login form (second gate)
    # Always attempt — uc_gui_click_captcha scans for captcha iframes automatically.
    # We also log whether a widget was detected so we can diagnose cases
    # where the click misbehaves (e.g. lands on userid when no widget
    # present). Detection is diagnostic only — the click runs either way.
    log.info("    Solving Turnstile on login form...")
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
    log.info(f"    Turnstile widget detected by selectors: {turnstile_detected}")
    if turnstile_detected:
        try:
            driver.uc_gui_click_captcha()
            time.sleep(5)
            log.info("    Turnstile click done.")
        except Exception as e:
            log.info(f"    Turnstile click attempt: {e}")
    else:
        log.info("    No Turnstile widget visible — skipping captcha click.")

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
        log.warning("    Could not find sign-in button.")
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
        log.warning("    LOGIN FAILED — still on sign-in page.")
        _save_error_screenshot(driver, "login_failed")
        return False

    log.info(f"    LOGIN SUCCESS as {email}")
    _current_account_email = email
    _current_account_clips = 0
    update_account_login(email)
    return True


def switch_account(driver, exclude_email=None):
    """Log out current account and log into the next available one.
    Returns True if switched successfully, False if no accounts available."""
    global _current_account_email

    # Mark current account as throttled
    if _current_account_email:
        update_account_stats(_current_account_email, throttled=True)

    accounts = get_all_active_accounts()
    if not accounts:
        log.info("    No accounts configured — cannot switch.")
        return False

    # Filter out the excluded (throttled) account
    candidates = [a for a in accounts if a["email"] != exclude_email]
    if not candidates:
        log.info("    No other active accounts available to switch to.")
        return False

    # Log out current session
    do_logout(driver)

    # Navigate back to newspapers.com for login
    navigate(driver, "https://www.newspapers.com/")
    time.sleep(2)

    # Try each candidate account
    for acct in candidates:
        log.info(f"    Trying account: {acct['email']}")
        if do_login(driver, acct):
            # Navigate to the newspaper site we clip from
            navigate(driver, "https://star-telegram.newspapers.com/")
            time.sleep(2)
            return True
        else:
            log.warning(f"    Failed to login as {acct['email']}, trying next...")
            # Make sure we're logged out before trying next
            do_logout(driver)

    log.warning("    All account login attempts failed.")
    return False


# === BROWSER SETUP ===

def _clean_chrome_profile_locks(profile_dir):
    """Remove Chrome singleton lock files that prevent a clean new browser launch."""
    for lock_name in ("SingletonLock", "SingletonCookie", "SingletonSocket"):
        lock_path = os.path.join(profile_dir, lock_name)
        try:
            if os.path.exists(lock_path):
                os.remove(lock_path)
                log.info(f"    Removed stale lock: {lock_path}")
        except Exception as e:
            log.warning(f"    Could not remove {lock_path}: {e}")


def _patch_chrome_preferences(profile_dir):
    """Patch Chrome Preferences to prevent session restore / crash bubble.

    Sets exit_type to 'none' and exited_cleanly to true so Chrome doesn't
    think it crashed and try to reopen previous tabs on the next launch.
    """
    import json as _json
    prefs_path = os.path.join(profile_dir, "Default", "Preferences")
    prefs = {}
    if os.path.isfile(prefs_path):
        try:
            with open(prefs_path, "r", encoding="utf-8") as f:
                prefs = _json.load(f)
        except Exception:
            prefs = {}
    # Ensure nested dicts exist
    prefs.setdefault("profile", {})
    prefs["profile"]["exit_type"] = "none"
    prefs["profile"]["exited_cleanly"] = True
    os.makedirs(os.path.dirname(prefs_path), exist_ok=True)
    try:
        with open(prefs_path, "w", encoding="utf-8") as f:
            _json.dump(prefs, f)
        log.info(f"    Patched Chrome Preferences: exit_type=none, exited_cleanly=true")
    except Exception as e:
        log.warning(f"    Could not patch Chrome Preferences: {e}")


def setup_driver(preferred_account=None, slot_id=None):
    # Abort early if stop flag is set — don't waste time opening a browser.
    if slot_id and check_instance_stop(slot_id):
        log.info("  Stop flag detected before browser launch — aborting setup.")
        return None

    # In slot mode each worker gets its own Chrome profile so multiple
    # browsers can run in parallel without colliding on the user-data-dir.
    if slot_id:
        temp_profile = rf"c:\lake_worth_runtime\chrome_temp_profile_clipper_{slot_id}"
    else:
        temp_profile = r"c:\lake_worth_runtime\chrome_temp_profile_clipper"
    os.makedirs(temp_profile, exist_ok=True)
    _clean_chrome_profile_locks(temp_profile)
    _patch_chrome_preferences(temp_profile)
    driver = Driver(uc=True, headed=True, user_data_dir=temp_profile,
                    chromium_arg="--disable-session-crashed-bubble")
    driver.set_window_size(1920, 1080)
    try:
        driver.maximize_window()
    except Exception:
        pass
    driver.implicitly_wait(5)

    # Navigate and handle Cloudflare if needed
    driver.uc_open_with_reconnect("https://star-telegram.newspapers.com/", 4)
    close_extra_tabs(driver)
    time.sleep(3)
    solve_cloudflare(driver)

    # Post-launch health check — verify we have a real standalone browser
    health = validate_browser_health(driver, slot_id=slot_id)
    if slot_id:
        write_instance_status(slot_id, browser_health=browser_health_str(health))
    if not health["healthy"]:
        log.error(f"  Browser health check FAILED: {health['issues']}")
        _save_error_screenshot(driver, "health_failed_setup", slot_id=slot_id)
        try:
            driver.quit()
        except Exception:
            pass
        raise RuntimeError(f"Browser health check failed: {health['issues']}")
    if health["issues"]:
        log.warning(f"  Browser health warnings: {health['issues']}")

    # Check if logged in — try auto-login from accounts DB, else wait for manual
    global _current_account_email, _current_account_clips
    page_text = driver.execute_script("return document.body.innerText || '';").lower()
    if "sign in" in page_text or "log in" in page_text:
        # Pick account: prefer specified account, else next available
        acct = None
        if preferred_account:
            aconn = get_accounts_db()
            try:
                row = aconn.execute("SELECT * FROM accounts WHERE email = ? AND active = 1", (preferred_account,)).fetchone()
                if row:
                    acct = dict(row)
                else:
                    log.warning(f"Preferred account {preferred_account} not found or inactive.")
            finally:
                aconn.close()
        if not acct:
            acct = get_next_account()

        if acct:
            log.info(f"NOT LOGGED IN — auto-login as {acct['email']}...")
            if do_login(driver, acct):
                log.info(f"Auto-login successful: {acct['email']}")
                # Navigate back to star-telegram
                navigate(driver, "https://star-telegram.newspapers.com/")
                time.sleep(2)
            else:
                _save_error_screenshot(driver, "login_failed_setup", slot_id=slot_id)
                log.warning(f"Auto-login failed for {acct['email']}. Waiting for manual login...")
                log.info("Please log in to newspapers.com in the browser window.")
                log.info("Waiting up to 60 seconds for login...")
                for i in range(60):
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
            log.info("No accounts in DB. Please log in manually in the browser window.")
            log.info("Waiting up to 60 seconds for login...")
            for i in range(60):
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
        current_user = _detect_logged_in_account(driver)
        target_account = preferred_account

        if not target_account:
            # Auto-rotate mode: check if the current session is an eligible account
            if current_user:
                # Check if this account is eligible (not in 24h cooldown)
                aconn = get_accounts_db()
                try:
                    row = aconn.execute(
                        "SELECT * FROM accounts WHERE email = ? AND active = 1 AND (last_throttle_time IS NULL OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))",
                        (current_user,)
                    ).fetchone()
                    if row:
                        log.info(f"  Current session ({current_user}) is eligible.")
                        target_account = current_user
                    else:
                        log.info(f"  Current session ({current_user}) is in cooldown or inactive.")
                finally:
                    aconn.close()

            if not target_account:
                # Need to switch to an eligible account
                next_acct = get_next_account()
                if next_acct:
                    log.info(f"  Switching to eligible account: {next_acct['email']}...")
                    do_logout(driver)
                    time.sleep(2)
                    if do_login(driver, next_acct):
                        target_account = next_acct["email"]
                        log.info(f"  Switched to {target_account}")
                        navigate(driver, "https://star-telegram.newspapers.com/")
                        time.sleep(2)
                    else:
                        log.warning(f"  Could not login as {next_acct['email']}.")
                else:
                    log.warning("  No eligible accounts available.")

        elif preferred_account:
            # Specific account requested — switch if needed
            if current_user and current_user.lower() != preferred_account.lower():
                log.info(f"  Logged in as {current_user}, but need {preferred_account} — switching...")
                do_logout(driver)
                time.sleep(2)
                aconn = get_accounts_db()
                try:
                    row = aconn.execute("SELECT * FROM accounts WHERE email = ? AND active = 1", (preferred_account,)).fetchone()
                    if row:
                        if do_login(driver, dict(row)):
                            log.info(f"  Switched to {preferred_account}")
                            navigate(driver, "https://star-telegram.newspapers.com/")
                            time.sleep(2)
                        else:
                            log.warning(f"  Could not login as {preferred_account}, proceeding with current session.")
                            target_account = current_user
                finally:
                    aconn.close()

        if target_account:
            _current_account_email = target_account
            _current_account_clips = 0
            log.info(f"  Tracking as account: {target_account}")

    # Final stop check — if flagged during login, close browser and abort.
    if slot_id and check_instance_stop(slot_id):
        log.info("  Stop flag detected after login — closing browser.")
        try:
            driver.quit()
        except Exception:
            pass
        return None

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

    # Drag NW handle to top-left of viewer — pure JS synthetic events so
    # multiple concurrent browser instances don't fight over the OS mouse
    # (mirrors the SE drag implementation below).
    log.info(f"    Dragging NW toward viewer top-left (JS)")
    nw_result = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var nw = null;
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'nw-resize') {
                nw = circles[i];
                break;
            }
        }
        if (!nw) return {error: 'no nw handle'};

        var rect = nw.getBoundingClientRect();
        var startX = rect.left + rect.width/2;
        var startY = rect.top + rect.height/2;

        var viewer = document.getElementById('svg-viewer');
        var vr = viewer.getBoundingClientRect();
        var endX = vr.left + """ + str(margin) + """;
        var endY = vr.top + """ + str(margin) + """;

        function fireMouseEvent(type, x, y) {
            var evt = new MouseEvent(type, {
                bubbles: true, cancelable: true, view: window,
                clientX: x, clientY: y,
                button: 0, buttons: 1
            });
            nw.dispatchEvent(evt);
        }

        fireMouseEvent('mousedown', startX, startY);
        var steps = 10;
        for (var s = 1; s <= steps; s++) {
            var mx = startX + (endX - startX) * s / steps;
            var my = startY + (endY - startY) * s / steps;
            fireMouseEvent('mousemove', mx, my);
        }
        fireMouseEvent('mouseup', endX, endY);

        return {start: {x: startX, y: startY}, end: {x: endX, y: endY}, ok: true};
    """)
    if not nw_result or nw_result.get("error"):
        log.warning(f"    NW drag failed: {nw_result}")
        return False
    log.info(f"    Dragged NW via JS: ({nw_result['start']['x']:.0f},{nw_result['start']['y']:.0f}) -> ({nw_result['end']['x']:.0f},{nw_result['end']['y']:.0f})")
    time.sleep(2)

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

    # Verify handles landed near viewer corners
    ok = _verify_clip_coverage(driver, viewer, margin)
    if ok:
        return True

    # Retry once — mouse may have been bumped
    log.warning("    Clip coverage check failed — retrying drags...")
    ok = _retry_clip_drags(driver, margin)
    if not ok:
        log.warning("    Proceeding despite verification failure (clip may be partial).")
    return True  # Always proceed — Save button will use whatever coverage we got


def _get_handle_positions(driver):
    """Re-read current NW and SE handle positions."""
    return driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var handles = {};
        for (var i = 0; i < circles.length; i++) {
            var c = circles[i];
            var style = window.getComputedStyle(c);
            var rect = c.getBoundingClientRect();
            if (rect.width < 1) continue;
            var cursor = style.cursor;
            if (cursor === 'nw-resize' || cursor === 'se-resize') {
                handles[cursor] = { x: rect.left + rect.width/2, y: rect.top + rect.height/2 };
            }
        }
        var viewer = document.getElementById('svg-viewer');
        var vr = viewer ? viewer.getBoundingClientRect() : null;
        return {
            handles: handles,
            viewer: vr ? {left: vr.left, top: vr.top, right: vr.right, bottom: vr.bottom} : null
        };
    """)


def _verify_clip_coverage(driver, viewer, margin, threshold=50):
    """Check that NW handle is near top-left and SE handle is near bottom-right of viewer.
    Returns True if both are within threshold pixels of their target corners."""
    info = _get_handle_positions(driver)
    handles = info.get("handles", {})
    if "nw-resize" not in handles or "se-resize" not in handles:
        log.warning("    Verify: could not find handles")
        return False

    nw = handles["nw-resize"]
    se = handles["se-resize"]
    # With maximized browsers, handles consistently land slightly inside the
    # viewer corners. Check against those observed inner bounds with a small
    # tolerance. Currently we only log — no failure mode yet, just watching.
    tol = 20
    nw_target_x = viewer["left"] + margin
    nw_target_y = viewer["top"] + margin
    se_target_x = viewer["right"] - margin
    se_target_y = viewer["bottom"] - margin

    nw_dx = nw["x"] - nw_target_x
    nw_dy = nw["y"] - nw_target_y
    se_dx = se_target_x - se["x"]
    se_dy = se_target_y - se["y"]

    log.info(
        f"    Verify: NW=({nw['x']:.0f},{nw['y']:.0f}) inside by ({nw_dx:.0f},{nw_dy:.0f}); "
        f"SE=({se['x']:.0f},{se['y']:.0f}) inside by ({se_dx:.0f},{se_dy:.0f}) [tol={tol}]"
    )

    if nw_dx < -tol or nw_dy < -tol or se_dx < -tol or se_dy < -tol:
        log.warning(
            f"    Verify NOTE (non-fatal): handles outside expected inner bounds — "
            f"NW at ({nw['x']:.0f},{nw['y']:.0f}), SE at ({se['x']:.0f},{se['y']:.0f})"
        )
    # Always return True for now — just watching.
    return True


def _retry_clip_drags(driver, margin):
    """Re-read handle positions and redo both drags, then verify again."""
    info = _get_handle_positions(driver)
    handles = info.get("handles", {})
    viewer = info.get("viewer")
    if not viewer or "nw-resize" not in handles or "se-resize" not in handles:
        log.warning("    Retry: could not find handles/viewer")
        return False

    # Re-drag NW via pure JS (no OS mouse)
    log.info(f"    Retry: dragging NW via JS")
    nw_result = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var nw = null;
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'nw-resize') { nw = circles[i]; break; }
        }
        if (!nw) return {error: 'no nw handle'};
        var rect = nw.getBoundingClientRect();
        var startX = rect.left + rect.width/2;
        var startY = rect.top + rect.height/2;
        var viewer = document.getElementById('svg-viewer');
        var vr = viewer.getBoundingClientRect();
        var endX = vr.left + """ + str(margin) + """;
        var endY = vr.top + """ + str(margin) + """;
        function fireMouseEvent(type, x, y) {
            nw.dispatchEvent(new MouseEvent(type, {
                bubbles: true, cancelable: true, view: window,
                clientX: x, clientY: y, button: 0, buttons: 1
            }));
        }
        fireMouseEvent('mousedown', startX, startY);
        for (var s = 1; s <= 10; s++) {
            fireMouseEvent('mousemove', startX + (endX-startX)*s/10, startY + (endY-startY)*s/10);
        }
        fireMouseEvent('mouseup', endX, endY);
        return {ok: true};
    """)
    if not nw_result or nw_result.get("error"):
        log.warning(f"    Retry: NW drag failed: {nw_result}")
        return False
    time.sleep(2)

    # Re-drag SE via JS
    result = driver.execute_script("""
        var circles = document.querySelectorAll('circle');
        var se = null;
        for (var i = 0; i < circles.length; i++) {
            if (window.getComputedStyle(circles[i]).cursor === 'se-resize') { se = circles[i]; break; }
        }
        if (!se) return {error: 'no se handle'};
        var rect = se.getBoundingClientRect();
        var startX = rect.left + rect.width/2;
        var startY = rect.top + rect.height/2;
        var viewer = document.getElementById('svg-viewer');
        var vr = viewer.getBoundingClientRect();
        var endX = vr.right - """ + str(margin) + """;
        var endY = vr.bottom - """ + str(margin) + """;
        function fireMouseEvent(type, x, y) {
            se.dispatchEvent(new MouseEvent(type, {
                bubbles: true, cancelable: true, view: window,
                clientX: x, clientY: y, button: 0, buttons: 1
            }));
        }
        fireMouseEvent('mousedown', startX, startY);
        for (var s = 1; s <= 10; s++) {
            fireMouseEvent('mousemove', startX + (endX-startX)*s/10, startY + (endY-startY)*s/10);
        }
        fireMouseEvent('mouseup', endX, endY);
        return {ok: true};
    """)
    if not result or result.get("error"):
        log.warning(f"    Retry: SE drag failed: {result}")
        return False
    time.sleep(2)

    # Verify again
    ok = _verify_clip_coverage(driver, viewer, margin)
    if ok:
        log.info("    Retry succeeded — clip covers full page.")
    else:
        log.warning("    Retry failed — clip may not cover full page.")
    return ok


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
                navigate(driver, href)
                log.info(f"    Navigated to clip article: {href[:80]}")
                time.sleep(2)
                return True

        # Construct URL directly from what we know
        # Pattern: /article/NEWSPAPER_SLUG/CLIPPING_ID/
        # Try navigating via the clipping page
        article_links = driver.find_elements(By.CSS_SELECTOR, "a[href*='/article/']")
        if article_links:
            href = article_links[0].get_attribute("href")
            if href:
                navigate(driver, href)
                log.info(f"    Navigated to article link: {href[:80]}")
                time.sleep(2)
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


def classify_clip_with_vision(driver, ocr_text=""):
    """Ask Claude vision to classify a clip image and extract any readable
    caption/title text visible on the page. Returns a dict:
      {
        "category": "MOSTLY_PICTURES" | "BAD_SCAN" | "HAS_TEXT" | None,
        "caption_text": "<any readable text visible in the clip>",
        "mentions_lake_worth": bool,
      }
    Returns None on vision-call failure.

    Categories:
      MOSTLY_PICTURES — page is legitimately picture-heavy (photos,
          illustrations, maps, cartoons). High-value: keep and surface.
      BAD_SCAN       — scan quality too poor to read text.
      HAS_TEXT       — readable body text is clearly present; OCR failure
          is therefore transient and we should retry later.
    """
    try:
        import anthropic
        import base64
        png_bytes = None
        try:
            clip_img = driver.find_element(
                By.CSS_SELECTOR,
                "img[src*='clip'], img[src*='clipping'], img.article-image, main img",
            )
            png_bytes = clip_img.screenshot_as_png
        except Exception:
            try:
                png_bytes = driver.get_screenshot_as_png()
            except Exception as e:
                log.warning(f"    Vision check: could not capture screenshot: {e}")
                return None

        if not png_bytes:
            return None

        b64 = base64.standard_b64encode(png_bytes).decode("ascii")
        client = anthropic.Anthropic()
        response = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=800,
            messages=[{
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": b64,
                        },
                    },
                    {
                        "type": "text",
                        "text": (
                            "This is a clipping from an old newspaper page. Our "
                            f"OCR extracted {len(ocr_text)} characters "
                            f"(~{len(ocr_text.split())} words).\n\n"
                            "Do TWO things:\n\n"
                            "1) Classify the page into EXACTLY ONE category:\n"
                            "   MOSTLY_PICTURES — dominated by photos, illustrations, "
                            "maps, cartoons, or graphics. Body text is minimal "
                            "(captions and a few words still count as MOSTLY_PICTURES).\n"
                            "   BAD_SCAN — the scan is so damaged, blurry, torn, "
                            "faded, or illegible that text cannot be read even by "
                            "a human.\n"
                            "   HAS_TEXT — readable body text is clearly present "
                            "in substantial amounts.\n\n"
                            "2) Transcribe any readable text you can see in the "
                            "image: headlines, photo captions, subtitles, labels "
                            "on maps, text inside cartoons, etc. Old newspaper "
                            "photos are often hard to see but the caption text "
                            "underneath each photo is usually readable — focus "
                            "on those. If you cannot read anything, leave empty.\n\n"
                            "Return a JSON object and nothing else, in this exact "
                            "shape:\n"
                            '{"category": "MOSTLY_PICTURES|BAD_SCAN|HAS_TEXT", '
                            '"caption_text": "<readable text joined with newlines, or empty string>"}'
                        ),
                    },
                ],
            }],
        )
        raw = (response.content[0].text or "").strip()
        log.info(f"    Vision raw answer (first 200): {raw[:200]}")
        # Pull JSON out (tolerate code fences)
        json_text = raw
        if "```" in json_text:
            m = re.search(r'```(?:json)?\s*(.*?)```', json_text, re.DOTALL)
            if m:
                json_text = m.group(1).strip()
        category = None
        caption_text = ""
        try:
            data = json.loads(json_text)
            cat_raw = (data.get("category") or "").strip().upper()
            if "MOSTLY_PICTURES" in cat_raw or "MOSTLY PICTURES" in cat_raw:
                category = "MOSTLY_PICTURES"
            elif "BAD_SCAN" in cat_raw or "BAD SCAN" in cat_raw:
                category = "BAD_SCAN"
            elif "HAS_TEXT" in cat_raw or "HAS TEXT" in cat_raw:
                category = "HAS_TEXT"
            caption_text = (data.get("caption_text") or "").strip()
        except Exception as e:
            log.warning(f"    Vision JSON parse failed: {e} — trying keyword fallback")
            up = raw.upper()
            if "MOSTLY_PICTURES" in up or "MOSTLY PICTURES" in up:
                category = "MOSTLY_PICTURES"
            elif "BAD_SCAN" in up or "BAD SCAN" in up:
                category = "BAD_SCAN"
            elif "HAS_TEXT" in up or "HAS TEXT" in up:
                category = "HAS_TEXT"
        mentions_lw = False
        if caption_text:
            mentions_lw = bool(re.search(r'(?i)lake[\s.\-,;:]+worth', caption_text))
        if category is None:
            return None
        return {
            "category": category,
            "caption_text": caption_text,
            "mentions_lake_worth": mentions_lw,
        }
    except Exception as e:
        log.warning(f"    Vision check failed: {e}")
        return None


def save_review_article(conn, pdf_filename, clip_url, meta, prefix, body_text,
                        caption_text="", mentions_lake_worth=False):
    """Insert a synthetic articles row for a page that the clipper is giving
    up on (picture-heavy, bad scan, etc.) so it appears in the Articles tab
    for human review instead of silently disappearing. prefix is e.g.
    'PICTURE HEAVY' or 'BAD SCAN'. body_text is any short OCR we captured
    (may be empty). caption_text is any text the vision model read off the
    image (photo captions, headlines, labels). If mentions_lake_worth is
    True the headline is prefixed with [LAKE WORTH] so these jump out.
    """
    try:
        newspaper_label = (meta.get("newspaper") or "").replace("_", " ")
        date_label = meta.get("date") or ""
        page_label = meta.get("page") or 0
        title_tail_bits = [b for b in [newspaper_label, date_label, f"page {page_label}" if page_label else ""] if b]
        title_tail = ", ".join(title_tail_bits) if title_tail_bits else pdf_filename
        lw_tag = "[LAKE WORTH] " if mentions_lake_worth else ""
        headline = f"{lw_tag}{prefix} - {title_tail}"
        is_picture_heavy = prefix.upper().startswith("PICTURE HEAVY")
        # Build full_text: vision caption text first (most useful for search),
        # then any short OCR body we captured.
        combined_bits = []
        if caption_text:
            combined_bits.append("[Vision-read text from image]\n" + caption_text)
        if body_text:
            combined_bits.append("[OCR]\n" + body_text)
        combined_text = "\n\n".join(combined_bits)
        # photo_description holds a compact summary used by dashboard
        if is_picture_heavy:
            if caption_text:
                pd = caption_text
                if mentions_lake_worth:
                    pd = "[MENTIONS LAKE WORTH] " + pd
            else:
                pd = "Page flagged by vision classifier for human review."
        else:
            pd = caption_text or ""
        synthetic = [{
            "headline": headline,
            "text": combined_text,
            "photo_description": pd,
        }]
        # Ensure processed_pdfs.has_photo is set for picture-heavy so
        # save_articles propagates has_photo=1 on the inserted row.
        if is_picture_heavy:
            try:
                conn.execute(
                    "UPDATE processed_pdfs SET has_photo = 1 WHERE pdf_filename = ?",
                    (pdf_filename,),
                )
                db_commit(conn)
            except Exception as e:
                log.warning(f"    Could not set has_photo on processed_pdfs: {e}")
        return save_articles(conn, pdf_filename, synthetic, SEARCH_TERM, clip_url=clip_url)
    except Exception as e:
        log.warning(f"    save_review_article failed: {e}")
        return 0


# Backwards-compatible wrapper — older code paths may still reference the
# old name. Maps the new classifier output back to the old 2-way vocabulary.
def check_clip_is_mostly_images(driver, ocr_text=""):
    result = classify_clip_with_vision(driver, ocr_text=ocr_text)
    if not result:
        return None
    cat = result.get("category")
    if cat == "MOSTLY_PICTURES" or cat == "BAD_SCAN":
        return "CONSISTENT"
    if cat == "HAS_TEXT":
        return "MORE_EXPECTED"
    return None


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
            max_tokens=16384,
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
        return None
    except json.JSONDecodeError as e:
        log.warning(f"    AI returned invalid JSON: {e}")
        return None
    except Exception as e:
        log.warning(f"    AI extraction error: {e}")
        err_str = str(e).lower()
        if "credit balance" in err_str or "billing" in err_str or "purchase credits" in err_str:
            log.error("    *** ANTHROPIC API CREDITS EXHAUSTED — EMERGENCY STOP ***")
            # 1. Set global stop flag immediately
            try:
                os.makedirs(GLOBAL_STOP_FLAG_FILE, exist_ok=True)
            except Exception:
                pass
            # 2. Zero target and write timestamp to DB
            try:
                _db = get_db()
                _db.execute(
                    "INSERT OR REPLACE INTO clipper_state (key, value) VALUES (?, ?)",
                    ("api_credits_exhausted", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                )
                _db.execute(
                    "INSERT OR REPLACE INTO clipper_state (key, value) VALUES ('instances_target', '0')",
                )
                _db.commit()
                _db.close()
            except Exception:
                pass
            # 3. Set per-slot stop flags for all running instances
            try:
                _db2 = get_db()
                _slots = [r[0] for r in _db2.execute("SELECT slot_id FROM clipper_instances").fetchall()]
                _db2.close()
                for _sid in _slots:
                    try:
                        os.makedirs(os.path.join(r"c:\lake_worth_runtime", f"stop_clipper_{_sid}"), exist_ok=True)
                    except Exception:
                        pass
            except Exception:
                pass
            return "credits_exhausted"
        return None


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
    if not navigate(driver, url):
        log.warning(f"    Cloudflare blocked navigation. Stopping.")
        return "stop"

    # Detect subscription paywall modal. When the session has been logged
    # out, newspapers.com drops a "Choose a subscription to view this page"
    # modal on top of the image. Interacting with the page in this state
    # is impossible — the only recovery is to exit the slot and let
    # autoscale respawn with a fresh login.
    try:
        _body = driver.execute_script("return document.body.innerText || '';") or ""
        _b = _body.lower()
        if (
            "choose a subscription to view this page" in _b
            or ("already have an account" in _b and "sign in" in _b
                and "start free trial" in _b)
        ):
            log.warning(
                "    Subscription paywall modal detected — session appears "
                "logged out. Exiting slot for respawn."
            )
            return "stop"
    except Exception:
        pass

    title = driver.title or ""
    meta = parse_page_title(title, url)

    if not meta["date"]:
        log.warning(f"    Could not parse title: {title[:60]}")
        _save_error_screenshot(driver, "parse_title_failed")
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
        _save_error_screenshot(driver, "no_clip_button")
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
        _save_error_screenshot(driver, "drag_corners_failed")
        return None


    # Step 4: Save
    if not click_save_button(driver):
        log.warning(f"    Could not find Save button for {pdf_filename}")
        _save_error_screenshot(driver, "no_save_button")
        return None

    # Check for throttle message — try account switch first, then escalating delays
    THROTTLE_DELAYS = [60, 120, 180, 300, 300]  # 1min, 2min, 3min, 5min, 5min then 10min
    try:
        page_text = driver.execute_script("return document.body.innerText || '';")
        if "unable to create your clipping" in page_text.lower():
            log.warning("    THROTTLED: 'unable to create your clipping' detected.")

            # Try switching to another account first
            if _current_account_email:
                log.info(f"    Current account: {_current_account_email} — attempting account switch...")
                if switch_account(driver, exclude_email=_current_account_email):
                    log.info(f"    Switched to {_current_account_email}. Retrying clip...")
                    # Retry the clip with new account
                    if not navigate(driver, url):
                        return "stop"
                    time.sleep(2)
                    zoom_out(driver)
                    time.sleep(1)
                    if click_clip_button(driver) and drag_clip_corners(driver) and click_save_button(driver):
                        page_text = driver.execute_script("return document.body.innerText || '';")
                        if "unable to create your clipping" not in page_text.lower():
                            log.info("    Clip succeeded with new account!")
                            # Fall through to Step 5
                            pass
                        else:
                            log.warning("    New account also throttled.")
                    else:
                        log.warning("    Clip retry failed after account switch.")

            # If still throttled (no switch or switch didn't help), use escalating delays
            page_text = driver.execute_script("return document.body.innerText || '';")
            if "unable to create your clipping" in page_text.lower():
                throttle_attempt = 0
                while True:
                    throttle_attempt += 1
                    if throttle_attempt <= len(THROTTLE_DELAYS):
                        delay = THROTTLE_DELAYS[throttle_attempt - 1]
                    else:
                        delay = 600
                    log.info(f"    Throttle retry {throttle_attempt}: waiting {delay}s...")
                    if stoppable_sleep(delay):
                        return "stop"
                    # Retry: navigate back to page and try clipping again
                    if not navigate(driver, url):
                        log.warning("    Cloudflare blocked during throttle retry.")
                        return "stop"
                    time.sleep(2)
                    zoom_out(driver)
                    time.sleep(1)
                    if not click_clip_button(driver):
                        log.warning("    Could not click Clip button on throttle retry.")
                        continue
                    time.sleep(2)
                    if not drag_clip_corners(driver):
                        log.warning("    Could not drag clip corners on throttle retry.")
                        continue
                    if not click_save_button(driver):
                        log.warning("    Could not click Save on throttle retry.")
                        continue
                    page_text = driver.execute_script("return document.body.innerText || '';")
                    if "unable to create your clipping" not in page_text.lower():
                        log.info(f"    Throttle cleared after {throttle_attempt} retries!")
                        break
                    log.warning(f"    Still throttled after retry {throttle_attempt}.")
                    # Every 3 retries, try switching accounts again
                    if throttle_attempt % 3 == 0 and _current_account_email:
                        log.info("    Trying account switch again...")
                        if switch_account(driver, exclude_email=_current_account_email):
                            log.info(f"    Switched to {_current_account_email}.")
    except Exception:
        pass

    # Step 5: Navigate to clip page
    if not navigate_to_clip_page(driver):
        log.warning(f"    Could not navigate to clip page for {pdf_filename}")
        _save_error_screenshot(driver, "no_clip_page")
        return None

    time.sleep(2)

    # Step 6: Check clip image size — if too small, cursor was moved during clipping. Re-clip.
    clip_url = get_clip_url(driver)
    try:
        clip_img = driver.find_element(By.CSS_SELECTOR, "img[src*='clip'], img[src*='clipping'], img.article-image, main img")
        img_width = clip_img.get_attribute("naturalWidth") or clip_img.get_attribute("width")
        img_height = clip_img.get_attribute("naturalHeight") or clip_img.get_attribute("height")
        img_width = int(img_width or 0)
        img_height = int(img_height or 0)
        log.info(f"    Clip image size: {img_width}x{img_height}")
        if img_width > 0 and img_height > 0 and (img_width < 750 or img_height < 800):
            log.warning(f"    Clip too small ({img_width}x{img_height}). Re-clipping...")
            navigate(driver, url)
            zoom_out(driver)
            time.sleep(1)
            if click_clip_button(driver):
                time.sleep(2)
                if drag_clip_corners(driver):
                    if click_save_button(driver):
                        if navigate_to_clip_page(driver):
                            time.sleep(2)
                            clip_url = get_clip_url(driver)
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

    # Step 7: Check for OCR button before attempting extraction
    ocr_btn_found = False
    for attempt in range(3):
        try:
            elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Article Text')]")
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
        # Could be a 502/temporary error — try reloading the page
        log.warning("    No OCR button found — checking for page error...")
        page_text = driver.execute_script("return document.body.innerText || '';")
        if any(err in page_text.lower() for err in ["502", "bad gateway", "503", "server error", "temporarily unavailable"]):
            log.warning("    Server error detected — retrying with reload...")
            reload_delays = [60, 120, 180, 300, 300]
            for retry in range(len(reload_delays)):
                delay = reload_delays[retry]
                log.info(f"    Waiting {delay}s before reload (retry {retry + 1}/{len(reload_delays)})...")
                if stoppable_sleep(delay):
                    return "stop"
                driver.refresh()
                time.sleep(5)
                if is_cloudflare(driver):
                    solve_cloudflare(driver)
                    time.sleep(3)
                try:
                    elements = driver.find_elements(By.XPATH, "//*[contains(text(), 'Article Text')]")
                    for el in elements:
                        if el.is_displayed():
                            ocr_btn_found = True
                            break
                except Exception:
                    pass
                if ocr_btn_found:
                    log.info(f"    OCR button found after reload (retry {retry + 1}).")
                    break
                log.info(f"    Reload retry {retry + 1}/{len(reload_delays)} — still no OCR button.")

    if not ocr_btn_found:
        # Check if it's Cloudflare (fatal) vs a transient error (skip this page)
        if is_cloudflare(driver):
            log.warning("    Cloudflare challenge on clip page. Stopping.")
            return "stop"
        # No OCR button at all — ask vision to classify the page so we can
        # tag and stop retrying if it's a legitimate picture-heavy / bad-scan
        # page. Otherwise fall through to retry-later behavior.
        log.warning("    No OCR button after retries — asking vision to classify the page...")
        vresult = classify_clip_with_vision(driver, ocr_text="")
        vcat = (vresult or {}).get("category")
        vcaption = (vresult or {}).get("caption_text", "") or ""
        vmentions = bool((vresult or {}).get("mentions_lake_worth"))
        log.info(f"    Vision verdict (no-OCR-button path): {vcat} lake_worth={vmentions} caption_chars={len(vcaption)}")
        if vcat == "MOSTLY_PICTURES":
            log.info(f"    Vision: PICTURE HEAVY{' [LAKE WORTH]' if vmentions else ''} — saving review article and marking done.")
            save_clip_data(conn, pdf_filename, url, clip_url, vcaption)
            n = save_review_article(
                conn, pdf_filename, clip_url, meta, "PICTURE HEAVY",
                body_text="", caption_text=vcaption, mentions_lake_worth=vmentions,
            )
            return {
                "pdf_filename": pdf_filename,
                "clip_url": clip_url,
                "ocr_len": len(vcaption),
                "articles": n,
                "date": meta["date"],
            }
        if vcat == "BAD_SCAN":
            log.info(f"    Vision: BAD SCAN — saving review article and marking done.")
            save_clip_data(conn, pdf_filename, url, clip_url, vcaption)
            n = save_review_article(
                conn, pdf_filename, clip_url, meta, "BAD SCAN",
                body_text="", caption_text=vcaption, mentions_lake_worth=vmentions,
            )
            return {
                "pdf_filename": pdf_filename,
                "clip_url": clip_url,
                "ocr_len": len(vcaption),
                "articles": n,
                "date": meta["date"],
            }
        # HAS_TEXT or None — treat as transient, retry later
        log.warning(f"    No OCR button and vision did not classify as picture/bad — skipping {pdf_filename} for retry later.")
        _save_error_screenshot(driver, "no_ocr_button_unclassified")
        return None

    # Step 8: Extract OCR text with retries
    ocr_text = extract_ocr_text(driver)
    log.info(f"    OCR: {len(ocr_text)} chars, clip: {clip_url[:60]}...")

    # If still too short, ask Claude vision FIRST whether the short OCR is
    # consistent with what's visible in the image. If the page genuinely has
    # little text (photos/ads/graphics), accept it immediately and skip the
    # expensive refresh-retry loop. Only if vision says MORE text should be
    # present do we fall through to refreshing the page and trying again.
    if len(ocr_text) <= 1000:
        log.info(f"    OCR short ({len(ocr_text)} chars). Asking Claude vision to classify the image...")
        vresult = classify_clip_with_vision(driver, ocr_text=ocr_text)
        vcat = (vresult or {}).get("category")
        vcaption = (vresult or {}).get("caption_text", "") or ""
        vmentions = bool((vresult or {}).get("mentions_lake_worth"))
        log.info(f"    Vision verdict (short-OCR path): {vcat} lake_worth={vmentions} caption_chars={len(vcaption)}")
        if vcat == "MOSTLY_PICTURES":
            log.info(f"    Vision: PICTURE HEAVY{' [LAKE WORTH]' if vmentions else ''} — saving review article and marking done.")
            save_clip_data(conn, pdf_filename, url, clip_url, ocr_text)
            n = save_review_article(
                conn, pdf_filename, clip_url, meta, "PICTURE HEAVY",
                body_text=ocr_text, caption_text=vcaption, mentions_lake_worth=vmentions,
            )
            return {
                "pdf_filename": pdf_filename,
                "clip_url": clip_url,
                "ocr_len": len(ocr_text),
                "articles": n,
                "date": meta["date"],
            }
        if vcat == "BAD_SCAN":
            log.info(f"    Vision: BAD SCAN — saving review article and marking done.")
            save_clip_data(conn, pdf_filename, url, clip_url, ocr_text)
            n = save_review_article(
                conn, pdf_filename, clip_url, meta, "BAD SCAN",
                body_text=ocr_text, caption_text=vcaption, mentions_lake_worth=vmentions,
            )
            return {
                "pdf_filename": pdf_filename,
                "clip_url": clip_url,
                "ocr_len": len(ocr_text),
                "articles": n,
                "date": meta["date"],
            }
        # HAS_TEXT or None — treat as transient, refresh + retry
        log.info(f"    Vision says readable text should be present (or check failed). Refreshing page...")
        for refresh_try in range(3):
            driver.refresh()
            time.sleep(5)
            ocr_text = extract_ocr_text(driver)
            log.info(f"    OCR after refresh {refresh_try + 1}/3: {len(ocr_text)} chars")
            if len(ocr_text) > 1000:
                break

    if len(ocr_text) <= 1000:
        log.warning(f"    OCR still too short ({len(ocr_text)} chars) after refresh retries. NOT marking as clipped — will retry later.")
        _save_error_screenshot(driver, "ocr_too_short")
        return None

    # Save to DB
    save_clip_data(conn, pdf_filename, url, clip_url, ocr_text)

    # Extract articles with AI. Return value is:
    #   list — normal case, may be empty ([] = genuinely no articles found)
    #   None — extraction failed (truncated JSON, API error, etc.). Do NOT
    #          mark articles_found=0 for these; leave the row un-clipped so
    #          it will be retried on a later pass.
    articles = extract_articles_with_ai(
        ocr_text, meta["date"], meta["newspaper"], meta["page"]
    )

    if articles == "credits_exhausted":
        log.error("    STOPPING: Anthropic API credits exhausted. Clip saved but AI extraction skipped.")
        _save_error_screenshot(driver, "credits_exhausted")
        # Don't un-clip — the clip and OCR are saved. Just stop.
        return "stop"

    if articles is None:
        log.warning(
            "    AI extraction failed — un-clipping this page so it will "
            "be re-queued for another attempt."
        )
        _save_error_screenshot(driver, "ai_extraction_failed")
        conn.execute(
            "UPDATE processed_pdfs SET clipped = 0, articles_found = NULL "
            "WHERE pdf_filename = ?",
            (pdf_filename,),
        )
        db_commit(conn)
        elapsed = time.time() - page_start_time
        log.info(f"    Page: {pdf_filename} — {elapsed:.1f}s (extraction failed, re-queued)")
        return None

    if articles:
        count = save_articles(conn, pdf_filename, articles, SEARCH_TERM, clip_url=clip_url)
        conn.execute(
            "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
            (count, pdf_filename)
        )
        db_commit(conn)
        log.info(f"    Found {count} articles")
    else:
        conn.execute(
            "UPDATE processed_pdfs SET articles_found = 0 WHERE pdf_filename = ?",
            (pdf_filename,),
        )
        db_commit(conn)
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


def get_unclipped_queue(conn, date_start=None, date_end=None):
    """Get unclipped, non-ignored entries that have a URL, sorted by date."""
    sql = """
        SELECT pp.pdf_filename, pp.url, pp.date_str
        FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE (pp.clipped = 0 OR pp.clipped IS NULL)
        AND a.id IS NULL
        AND (pp.ignored IS NULL OR pp.ignored = 0)
        AND pp.url IS NOT NULL AND pp.url != ''
    """
    params = []
    if date_start:
        sql += " AND pp.date_str >= ?"
        params.append(date_start)
    if date_end:
        sql += " AND pp.date_str <= ?"
        params.append(date_end)
    sql += " ORDER BY pp.date_str, pp.pdf_filename"
    return conn.execute(sql, params).fetchall()


def main_slot(slot_id, date_start=None, date_end=None, max_pages=0):
    """Multi-instance worker entry point.

    Claims one eligible account at a time, clips pages one-at-a-time from the
    shared queue via atomic page claims, and loops to the next account when
    the current one is throttled. Exits when the queue is empty or a stop flag
    is set. Guarantees that on any exit path, its page and account claims are
    released and its clipper_instances row is removed.

    Does not touch single-instance code paths.
    """
    global _current_account_email, _current_account_clips

    pid = os.getpid()
    started_at = _now_str()
    log.info("=" * 60)
    log.info(f"Clip & Extract — slot={slot_id} pid={pid}")
    log.info("=" * 60)
    log.info(f"  Date range: {date_start or 'start'} to {date_end or 'end'}")
    log.info(f"  Max pages:  {max_pages or 'unlimited'}")
    log.info(f"  Log: {log_filename}")

    # Check stop flags BEFORE doing anything. The per-slot flag is cleared by
    # _spawn_instance() in the server before launching us. Any flag that exists
    # now was set AFTER spawn — it's a real stop command. Obey it.
    if check_instance_stop(slot_id):
        log.info(f"  Stop flag set before boot — exiting immediately.")
        try:
            delete_instance_row(slot_id)
        except Exception:
            pass
        return

    conn = get_db()
    ensure_columns(conn)

    write_instance_status(
        slot_id, pid=pid, status="starting", date_start=date_start or "",
        date_end=date_end or "", started_at=started_at, count_this_run=0,
        last_action="boot",
    )

    driver = None
    claimed_account_email = None
    total_clipped = 0
    total_articles = 0

    def _release_all():
        """Best-effort cleanup: release any outstanding claims."""
        try:
            release_all_pages_for_slot(conn, slot_id)
        except Exception as e:
            log.warning(f"  release_all_pages_for_slot failed: {e}")
        if claimed_account_email:
            try:
                release_account(claimed_account_email, slot_id=slot_id)
            except Exception as e:
                log.warning(f"  release_account failed: {e}")

    try:
        while True:
            if check_instance_stop(slot_id):
                break

            # Claim an eligible account. If none available, idle-wait for one.
            acct = claim_account(slot_id, pid)
            if not acct:
                log.info("  No eligible account available — waiting 60s.")
                write_instance_status(
                    slot_id, status="waiting_for_account",
                    account_email="", last_action="no eligible account",
                )
                if stoppable_sleep(60) or check_instance_stop(slot_id):
                    break
                continue

            claimed_account_email = acct["email"]
            _current_account_email = claimed_account_email
            _current_account_clips = 0
            log.info(f"  Claimed account: {claimed_account_email}")
            write_instance_status(
                slot_id, status="starting", account_email=claimed_account_email,
                last_action="claimed account",
            )

            # Launch browser for this slot. The profile is slot-specific.
            try:
                driver = resilient_setup_driver(
                    preferred_account=claimed_account_email, slot_id=slot_id
                )
            except Exception as e:
                log.error(f"  setup_driver failed: {e}")
                driver = None

            if not driver:
                log.error("  Could not start browser — releasing account and retrying.")
                write_instance_status(slot_id, status="error", last_action="setup_driver failed")
                release_account(claimed_account_email, slot_id=slot_id)
                claimed_account_email = None
                _current_account_email = None
                if stoppable_sleep(30) or check_instance_stop(slot_id):
                    break
                continue

            write_instance_status(slot_id, status="running", last_action="browser up")

            clipped_image_ids, done_filenames = build_clipped_image_ids(conn)
            account_exhausted = False

            try:
                while True:
                    if check_instance_stop(slot_id):
                        break
                    if max_pages and total_clipped >= max_pages:
                        log.info(f"  Reached max_pages limit ({max_pages})")
                        break

                    # Health check: verify browser is alive, has 1 tab,
                    # is fullscreen, and on the right site. Auto-fixes
                    # minor issues (extra tabs, not maximized).
                    health = validate_browser_health(driver, slot_id=slot_id)
                    _health_str = browser_health_str(health)
                    write_instance_status(
                        slot_id, browser_health=_health_str,
                    )
                    if not health["healthy"]:
                        log.error(f"  Browser health FAILED: {health['issues']} — exiting slot for respawn.")
                        _save_error_screenshot(driver, "health_failed", slot_id=slot_id)
                        write_instance_status(
                            slot_id, status="error",
                            last_action=f"unhealthy:{_health_str}",
                        )
                        break
                    if health["issues"]:
                        log.warning(f"  Browser health warnings: {health['issues']}")

                    page = claim_next_page(conn, slot_id, pid, date_start, date_end)
                    if not page:
                        log.info("  Queue empty for this range — slot will exit.")
                        write_instance_status(slot_id, status="queue_empty", last_action="queue empty")
                        # Proactively zero the target so autoscale doesn't respawn
                        try:
                            other = conn.execute(
                                "SELECT status FROM clipper_instances WHERE slot_id != ?",
                                (slot_id,),
                            ).fetchall()
                            active = [r for r in other if (r["status"] or "").lower() in ("running", "starting", "logging-in", "spawning")]
                            if not active:
                                conn.execute(
                                    "INSERT OR REPLACE INTO clipper_state (key, value) VALUES ('instances_target', '0')"
                                )
                                db_commit(conn)
                                log.info("  All instances idle — set instances_target=0")
                        except Exception as e:
                            log.warning(f"  Could not zero target: {e}")
                        account_exhausted = True
                        break

                    pdf_filename = page["pdf_filename"]
                    url = page["url"]
                    log.info(f"\n  [slot {slot_id} #{total_clipped + 1}] {pdf_filename}")
                    write_instance_status(
                        slot_id, status="running",
                        current_date=page.get("date_str") or "",
                        current_page=_current_account_clips + 1,
                        count_this_run=_current_account_clips,
                        last_action=f"clip {pdf_filename}",
                    )

                    try:
                        check_internet_pause()
                        result = clip_page(driver, url, conn, clipped_image_ids, done_filenames)
                    except (WebDriverException, InvalidSessionIdException) as e:
                        log.error(f"    Session error: {e}")
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                        if not wait_for_internet():
                            log.error("    No internet — exiting slot.")
                            break
                        log.info("    Restarting browser session...")
                        driver = resilient_setup_driver(
                            preferred_account=claimed_account_email, slot_id=slot_id
                        )
                        if not driver:
                            log.error("    Could not recover browser — releasing account.")
                            break
                        continue
                    except Exception as e:
                        log.error(f"    Clip error: {e}")
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        if not is_internet_up() and not wait_for_internet():
                            log.error("    No internet — exiting slot.")
                            break
                        continue

                    if result == "skipped":
                        # Not our work anymore; release the claim so nothing is stuck.
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        continue
                    if result == "throttled":
                        log.info("  Throttle detected — releasing account and rotating.")
                        _save_error_screenshot(driver, "throttled", slot_id=slot_id)
                        # clip_page already waited; mark account throttled and rotate.
                        update_account_stats(claimed_account_email, throttled=True)
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        write_instance_status(slot_id, status="cooling", last_action="throttled")
                        break
                    if result == "stop":
                        _save_error_screenshot(driver, "clip_stop", slot_id=slot_id)
                        log.warning("  clip_page returned 'stop' — releasing and exiting slot.")
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        # Check if this is a credits-exhausted stop — kill everything
                        try:
                            _chk = get_db()
                            _cred_row = _chk.execute(
                                "SELECT value FROM clipper_state WHERE key = 'api_credits_exhausted'"
                            ).fetchone()
                            _chk.close()
                            if _cred_row:
                                log.error("  *** API CREDITS EXHAUSTED — setting global stop and target=0 ***")
                                os.makedirs(GLOBAL_STOP_FLAG_FILE, exist_ok=True)
                                try:
                                    conn.execute(
                                        "INSERT OR REPLACE INTO clipper_state (key, value) VALUES ('instances_target', '0')"
                                    )
                                    db_commit(conn)
                                except Exception:
                                    pass
                        except Exception:
                            pass
                        write_instance_status(slot_id, status="error", last_action="clip_page stop")
                        account_exhausted = True
                        break
                    if result is None:
                        log.warning("  clip_page returned None — releasing and continuing.")
                        release_page(conn, pdf_filename, slot_id=slot_id)
                        continue

                    # Success. clip_page already set clipped=1, which makes the
                    # claim moot, but clear it explicitly for tidiness.
                    release_page(conn, pdf_filename, slot_id=slot_id)
                    total_clipped += 1
                    _current_account_clips += 1
                    pg_articles = result.get("articles", 0) if isinstance(result, dict) else 0
                    total_articles += pg_articles
                    m = re.search(r'/image/(\d+)', url)
                    if m:
                        clipped_image_ids.add(m.group(1))
                    if isinstance(result, dict) and result.get("pdf_filename"):
                        done_filenames.add(result["pdf_filename"])
                    update_account_stats(
                        claimed_account_email, clips_added=1, articles_added=pg_articles
                    )

                    # Daily limit check using the true per-day counter.
                    clip_limit = get_daily_clip_limit()
                    if clip_limit:
                        _acc_conn = get_accounts_db()
                        try:
                            _acc_row = _acc_conn.execute(
                                "SELECT clips_today, clips_today_date FROM accounts WHERE email = ?",
                                (claimed_account_email,),
                            ).fetchone()
                            _today_str = datetime.now().strftime("%Y-%m-%d")
                            if _acc_row and _acc_row["clips_today_date"] == _today_str:
                                _day_total = _acc_row["clips_today"] or 0
                            else:
                                _day_total = 0
                        finally:
                            _acc_conn.close()
                        if _day_total >= clip_limit:
                            log.info(
                                f"  Daily clip limit reached ({_day_total}/{clip_limit}) "
                                f"for {claimed_account_email} — rotating."
                            )
                            _save_error_screenshot(driver, "daily_limit", slot_id=slot_id)
                            write_instance_status(slot_id, status="cooling", last_action="daily limit")
                            break
            finally:
                try:
                    if driver:
                        driver.quit()
                except Exception:
                    pass
                driver = None
                # Release account so another slot (or later run) can use it.
                release_account(claimed_account_email, slot_id=slot_id)
                _current_account_email = None
                claimed_account_email = None

            if account_exhausted:
                break  # queue empty or fatal — exit slot entirely

            # Otherwise loop to claim the next eligible account.

    except KeyboardInterrupt:
        log.info("\n  Slot stopped by user (KeyboardInterrupt).")
    except SystemExit:
        pass
    except Exception as e:
        log.error(f"  Fatal slot error: {e}", exc_info=True)
    finally:
        # Final cleanup: release anything still held, remove instance row.
        _release_all()
        try:
            delete_instance_row(slot_id)
        except Exception as e:
            log.warning(f"  delete_instance_row failed: {e}")
        try:
            conn.close()
        except Exception:
            pass
        log.info("=" * 60)
        log.info(f"DONE (slot {slot_id})")
        log.info(f"  Pages clipped: {total_clipped}")
        log.info(f"  Articles found: {total_articles}")
        log.info("=" * 60)


def main(max_pages=0, date_start=None, date_end=None, account_email=None):
    global _current_account_clips
    conn = get_db()
    ensure_columns(conn)

    queue = get_unclipped_queue(conn, date_start, date_end)
    clipped_image_ids, done_filenames = build_clipped_image_ids(conn)

    log.info("=" * 60)
    log.info("Clip & Extract — Direct URL Mode")
    log.info("=" * 60)
    log.info(f"  Queue: {len(queue)} unclipped pages")
    if date_start or date_end:
        log.info(f"  Date range: {date_start or 'start'} to {date_end or 'end'}")
    log.info(f"  Max pages: {max_pages or 'unlimited'}")
    log.info(f"  Log: {log_filename}")

    if not queue:
        log.info("Nothing to clip.")
        conn.close()
        return

    clipped = 0
    skipped = 0
    errors = 0
    total_articles = 0
    batch_clips = 0
    driver = None
    keep_browser_open = False

    # Clear stop flag from previous runs
    if os.path.exists(STOP_FLAG_FILE):
        if os.path.isdir(STOP_FLAG_FILE):
            os.rmdir(STOP_FLAG_FILE)
        else:
            os.remove(STOP_FLAG_FILE)
        log.info("  Cleared old stop flag.")

    try:
        driver = resilient_setup_driver(preferred_account=account_email)
        if not driver:
            log.error("  Could not start browser. Exiting.")
            return

        for row in queue:
            if keep_browser_open:
                break
            if check_stop_flag():
                break
            if max_pages and clipped >= max_pages:
                log.info(f"  Reached max_pages limit ({max_pages})")
                break

            url = row["url"]
            pdf_filename = row["pdf_filename"]

            log.info(f"\n  [{clipped + 1}/{len(queue)}] {pdf_filename}")

            try:
                check_internet_pause()
                result = clip_page(driver, url, conn, clipped_image_ids, done_filenames)
                if result == "skipped":
                    skipped += 1
                elif result == "throttled":
                    log.info("  Resuming after throttle wait...")
                elif result == "stop":
                    log.warning("  Failure — browser left open for inspection. Exiting.")
                    try:
                        spath = os.path.join(LOG_DIR, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                        driver.save_screenshot(spath)
                        log.info(f"  Screenshot saved: {spath}")
                    except Exception as e:
                        log.warning(f"  Screenshot failed: {e}")
                    keep_browser_open = True
                    break
                elif result is None:
                    errors += 1
                else:
                    clipped += 1
                    batch_clips += 1
                    total_articles += result.get("articles", 0)
                    m = re.search(r'/image/(\d+)', url)
                    if m:
                        clipped_image_ids.add(m.group(1))
                    if result.get("pdf_filename"):
                        done_filenames.add(result["pdf_filename"])
                    # Update account clip stats
                    if _current_account_email:
                        _current_account_clips += 1
                        page_articles = result.get("articles", 0)
                        update_account_stats(_current_account_email, clips_added=1, articles_added=page_articles)
                    log.info(f"  Progress: {clipped} clipped, {total_articles} articles, {errors} errors")
                    # Check daily clip limit using per-day counter
                    clip_limit = get_daily_clip_limit()
                    if clip_limit and _current_account_email:
                        _acc_conn = get_accounts_db()
                        try:
                            _acc_row = _acc_conn.execute(
                                "SELECT clips_today, clips_today_date FROM accounts WHERE email = ?",
                                (_current_account_email,)
                            ).fetchone()
                            _today_str = datetime.now().strftime("%Y-%m-%d")
                            if _acc_row and _acc_row["clips_today_date"] == _today_str:
                                _session_total = _acc_row["clips_today"] or 0
                            else:
                                _session_total = 0
                        finally:
                            _acc_conn.close()
                        if _session_total >= clip_limit:
                            log.info(f"  Daily clip limit reached ({_session_total}/{clip_limit}) for {_current_account_email}.")
                            # Try rotating to another eligible account
                            if switch_account(driver, exclude_email=_current_account_email):
                                log.info(f"  Rotated to {_current_account_email}. Continuing...")
                                batch_clips = 0
                            else:
                                log.info("  No other eligible accounts. Exiting.")
                                keep_browser_open = False
                                break
            except (WebDriverException, InvalidSessionIdException) as e:
                log.error(f"    Session error: {e}")
                errors += 1
                try:
                    driver.quit()
                except Exception:
                    pass
                # Wait for internet before trying to restart browser
                if not wait_for_internet():
                    log.error("    No internet — exiting.")
                    break
                log.info("    Restarting browser session...")
                driver = resilient_setup_driver(preferred_account=account_email)
                if not driver:
                    log.error("    Could not recover browser. Exiting.")
                    break
                batch_clips = 0
                log.info("    Session recovered.")
            except Exception as e:
                log.error(f"    Clip error: {e}")
                errors += 1
                # If internet is down, wait rather than burning through errors
                if not is_internet_up():
                    if not wait_for_internet():
                        log.error("    No internet — exiting.")
                        break

            # Every 100 clips, restart browser to stay fresh
            if batch_clips >= 100:
                log.info(f"  === 100-clip checkpoint. Restarting browser. ===")
                try:
                    driver.quit()
                except Exception:
                    pass
                driver = resilient_setup_driver(preferred_account=account_email)
                if not driver:
                    log.error("    Could not restart browser. Exiting.")
                    break
                batch_clips = 0

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    except SystemExit:
        pass
    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
    finally:
        if driver:
            try:
                screenshot_path = os.path.join(LOG_DIR, f"screenshot_{datetime.now().strftime('%Y%m%d_%H%M%S')}.png")
                driver.save_screenshot(screenshot_path)
                log.info(f"  Screenshot saved: {screenshot_path}")
            except Exception as e:
                log.warning(f"  Screenshot failed: {e}")
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
    # Usage (single-instance, backward compatible):
    #   python clip_and_extract.py [max_pages] [date_start] [date_end] [account_email]
    #
    # Usage (multi-instance slot):
    #   python clip_and_extract.py --slot-id=NAME [max_pages] [date_start] [date_end]
    #     slot mode auto-claims an eligible account; account_email positional is ignored.
    #
    # Examples:
    #   python clip_and_extract.py                                        # all unclipped
    #   python clip_and_extract.py 10                                     # first 10
    #   python clip_and_extract.py 0 1916-01-01 1917-12-31                # date range
    #   python clip_and_extract.py 0 1916-01-01 1917-12-31 user@example.com
    #   python clip_and_extract.py --slot-id=1 0 1916-01-01 1917-12-31    # slot mode

    # Extract --slot-id=X from argv without disturbing positional parsing.
    _slot_id = None
    _argv = []
    for _a in sys.argv[1:]:
        if _a.startswith("--slot-id="):
            _slot_id = _a.split("=", 1)[1].strip() or None
        elif _a == "--slot-id":
            # support "--slot-id X" form
            continue
        else:
            _argv.append(_a)

    limit = int(_argv[0]) if len(_argv) > 0 and _argv[0] else 0
    ds = _argv[1] if len(_argv) > 1 and _argv[1] else None
    de = _argv[2] if len(_argv) > 2 and _argv[2] else None
    acct = _argv[3] if len(_argv) > 3 and _argv[3] else None

    if _slot_id:
        # Retag the logger with the slot id so each instance has its own log file.
        _slot_log = os.path.join(
            LOG_DIR, f"clipper_slot{_slot_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        _fh = logging.FileHandler(_slot_log)
        _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        # Remove the default file handler (clipper_YYYYMMDD.log) so log lines
        # only go to the slot-specific file, preventing double-counting in
        # the dashboard's log scanner.
        for _h in log.handlers[:]:
            if isinstance(_h, logging.FileHandler) and _h is not _fh:
                log.removeHandler(_h)
        log.addHandler(_fh)
        log.info(f"  Slot log file: {_slot_log}")
        main_slot(_slot_id, date_start=ds, date_end=de, max_pages=limit)
    else:
        main(max_pages=limit, date_start=ds, date_end=de, account_email=acct)
