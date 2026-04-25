"""
Collect search result URLs from newspapers.com.

Built on browser_session.py for proven login, account rotation, and
cooldown management — the same stack the clipper uses.

Scrapes search result pages to harvest image URLs and metadata,
creating entries in the database for the clipper to process.
Does NOT visit individual result pages — extracts metadata from
the search results DOM directly.

Usage (called by dashboard server):
    python collect_urls.py --start 1924-01-01 --end 1960-12-31
        [--max-urls 1500] [--restart-every 100] [--account user@email.com]
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

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys

from browser_session import (
    BrowserSession, close_extra_tabs, is_cloudflare, solve_cloudflare, navigate,
)

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

GLOBAL_STOP_FLAG = RUNTIME_DIR / "stop_collector_all"


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_collector_tables():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS collector_instances (
            slot_id        TEXT PRIMARY KEY,
            pid            INTEGER,
            account_email  TEXT,
            status         TEXT,
            current_date   TEXT,
            urls_this_run  INTEGER DEFAULT 0,
            date_start     TEXT,
            date_end       TEXT,
            started_at     TEXT,
            heartbeat_at   TEXT,
            last_action    TEXT
        )
    """)
    conn.commit()
    conn.close()


ensure_collector_tables()


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


def _now_str():
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def write_instance_status(slot_id, **fields):
    """Upsert this slot's row in collector_instances. Always bumps heartbeat_at."""
    if not slot_id:
        return
    allowed = {
        "pid", "account_email", "status", "current_date",
        "urls_this_run", "date_start", "date_end", "started_at", "last_action",
    }
    fields = {k: v for k, v in fields.items() if k in allowed}
    fields["heartbeat_at"] = _now_str()
    conn = get_db()
    try:
        existing = conn.execute(
            "SELECT slot_id FROM collector_instances WHERE slot_id = ?", (slot_id,)
        ).fetchone()
        if existing:
            if fields:
                sets = ", ".join(f"{k} = ?" for k in fields.keys())
                params = list(fields.values()) + [slot_id]
                conn.execute(
                    f"UPDATE collector_instances SET {sets} WHERE slot_id = ?",
                    params,
                )
        else:
            cols = ["slot_id"] + list(fields.keys())
            placeholders = ", ".join(["?"] * len(cols))
            params = [slot_id] + list(fields.values())
            conn.execute(
                f"INSERT INTO collector_instances ({', '.join(cols)}) VALUES ({placeholders})",
                params,
            )
        conn.commit()
    finally:
        conn.close()


def delete_instance_row(slot_id):
    """Remove this slot's row from collector_instances on shutdown."""
    if not slot_id:
        return
    conn = get_db()
    try:
        conn.execute("DELETE FROM collector_instances WHERE slot_id = ?", (slot_id,))
        conn.commit()
    finally:
        conn.close()


def instance_stop_flag_path(slot_id):
    return RUNTIME_DIR / f"stop_collector_{slot_id}"


def check_instance_stop(slot_id):
    """Return True if global, per-slot, or legacy stop flag is set."""
    if GLOBAL_STOP_FLAG.exists():
        log.info("  Global collector stop flag detected — exiting.")
        return True
    if slot_id and instance_stop_flag_path(slot_id).exists():
        log.info(f"  Stop flag for collector slot {slot_id} — exiting.")
        return True
    if STOP_FLAG.exists():
        log.info("  Legacy stop flag detected — exiting.")
        return True
    return False


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


def create_entry(conn, meta, thumbnail_path=None, gathered_by=""):
    """Create a processed_pdfs entry with metadata from search results.
    Returns True if a row was actually inserted, False if it was a duplicate."""
    cursor = conn.execute(
        "INSERT OR IGNORE INTO processed_pdfs "
        "(pdf_filename, articles_found, search_term, url, date_str, thumbnail_path, gathered_by) "
        "VALUES (?, 0, ?, ?, ?, ?, ?)",
        (meta["pdf_filename"], SEARCH_TERM, meta["url"], meta["date_str"], thumbnail_path, gathered_by),
    )
    conn.commit()
    return cursor.rowcount > 0


def update_url(conn, pdf_filename, url):
    """Backfill the URL for an existing entry if missing."""
    if url:
        conn.execute(
            "UPDATE processed_pdfs SET url = ? "
            "WHERE pdf_filename = ? AND (url IS NULL OR url = '')",
            (url, pdf_filename),
        )
        conn.commit()


def update_collector_account_stats(email, urls_added=0):
    """Update daily URL counter on the accounts table, matching the clipper's
    clips_today / clips_today_date pattern.

    Ensures columns: urls_today, urls_today_date, last_gathered_time, total_urls_gathered.
    Auto-resets urls_today to 0 when urls_today_date != today.
    """
    if not email or urls_added <= 0:
        return
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "accounts" not in tables:
            return
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "urls_today" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN urls_today INTEGER DEFAULT 0")
            conn.commit()
        if "urls_today_date" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN urls_today_date TEXT")
            conn.commit()
        if "last_gathered_time" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN last_gathered_time TEXT")
            conn.commit()
        if "total_urls_gathered" not in cols:
            conn.execute("ALTER TABLE accounts ADD COLUMN total_urls_gathered INTEGER DEFAULT 0")
            conn.commit()
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        today = datetime.now().strftime("%Y-%m-%d")
        # Reset urls_today if the stored date isn't today
        conn.execute("""
            UPDATE accounts SET urls_today = 0, urls_today_date = ?
             WHERE email = ? AND (urls_today_date IS NULL OR urls_today_date != ?)
        """, (today, email, today))
        # Increment daily and lifetime counters
        conn.execute("""
            UPDATE accounts SET
                urls_today = urls_today + ?,
                urls_today_date = ?,
                last_gathered_time = ?,
                total_urls_gathered = COALESCE(total_urls_gathered, 0) + ?
            WHERE email = ?
        """, (urls_added, today, now, urls_added, email))
        conn.commit()
    except Exception as e:
        log.warning(f"  update_collector_account_stats failed: {e}")
    finally:
        conn.close()


def is_collector_account_eligible(conn, email, max_urls):
    """Check if an account is eligible for URL collection using daily counters.
    Returns True if eligible (urls_today < max_urls OR stale date OR 24h+ since last gather)."""
    if not max_urls:
        return True
    try:
        tables = [r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()]
        if "accounts" not in tables:
            return True
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "urls_today" not in cols:
            return True  # columns not yet created, no tracking yet
        today = datetime.now().strftime("%Y-%m-%d")
        row = conn.execute(
            "SELECT urls_today, urls_today_date, last_gathered_time FROM accounts WHERE email = ?",
            (email,),
        ).fetchone()
        if not row:
            return True
        urls_today = row["urls_today"] or 0
        urls_today_date = row["urls_today_date"] or ""
        last_gathered = row["last_gathered_time"] or ""
        # Stale date means the counter hasn't been used today — eligible
        if urls_today_date != today:
            return True
        # Under limit — eligible
        if urls_today < max_urls:
            return True
        # Over limit but 24+ hours since last gather — eligible (safety valve)
        if last_gathered:
            try:
                last_dt = datetime.strptime(last_gathered, "%Y-%m-%d %H:%M:%S")
                if (datetime.now() - last_dt).total_seconds() >= 86400:
                    return True
            except Exception:
                pass
        return False
    except Exception:
        return True


def get_ineligible_collector_emails(conn, max_urls):
    """Return list of account emails that have hit today's URL limit.
    Used to build the exclude list for account rotation."""
    if not max_urls:
        return []
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        if "urls_today" not in cols:
            return []
        today = datetime.now().strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT email FROM accounts
            WHERE urls_today >= ?
              AND urls_today_date = ?
              AND last_gathered_time IS NOT NULL
              AND last_gathered_time >= datetime('now', 'localtime', '-24 hours')
        """, (max_urls, today)).fetchall()
        return [r[0] for r in rows]
    except Exception:
        return []


# === SEARCH RESULTS SCRAPING ===

def build_search_url(start_date, end_date):
    return (
        "https://star-telegram.newspapers.com/search/results/"
        f"?date-end={end_date}&date-start={start_date}"
        f"&keyword=%22lake+worth%22"
        "&sort=paper-date-asc"
    )


def scrape_results(driver):
    """Extract URLs and metadata from visible search results."""
    data = driver.execute_script("""
        var results = [];
        var links = document.querySelectorAll('a[href*="/image/"]');
        var seen = {};
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
            if (href.startsWith('/')) href = window.location.origin + href;
            seen[href] = true;
            var el = link;
            for (var i = 0; i < 6; i++) {
                if (el.parentElement) el = el.parentElement;
                var txt = el.innerText || '';
                if (txt.length > 30) break;
            }
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

    date_str = ""
    date_match = re.search(r'(\w{3,9})\s+(\d{1,2}),?\s+(\d{4})', text)
    if date_match:
        month_str = date_match.group(1)[:3].lower()
        month = MONTH_MAP.get(month_str, "")
        if month:
            day = date_match.group(2).zfill(2)
            year = date_match.group(3)
            date_str = f"{year}-{month}-{day}"

    page_match = re.search(r'page\s*(\d+)', text, re.IGNORECASE)
    page = int(page_match.group(1)) if page_match else 0

    newspaper = "Fort Worth Star Telegram"
    np_match = re.search(
        r'(Fort Worth (?:Star-Telegram|Record-Telegram|Record Telegram|'
        r'Star Telegram|Record))',
        text, re.IGNORECASE,
    )
    if np_match:
        newspaper = np_match.group(1)

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


def download_thumbnails(driver, entries):
    """Download thumbnail images via browser JS fetch."""
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


# === COLLECTION SESSION ===

def check_search_throttle(driver):
    """Check if newspapers.com has throttled searches from this IP.
    Returns True if throttled."""
    try:
        page_text = driver.execute_script(
            "return document.body.innerText || '';"
        ).lower()
        if "temporarily paused" in page_text or "paused searches" in page_text:
            return True
    except Exception:
        pass
    return False


def run_session(conn, driver, start_date, end_date, restart_every, gathered_by="",
                max_urls=0, account_urls_so_far=0,
                slot_id=None, total_new_before=0):
    """Collect URLs from search results. Returns (new_entries, latest_date)."""
    search_url = build_search_url(start_date, end_date)
    log.info(f"  Search: {start_date} to {end_date}")
    log.info(f"  URL: {search_url}")

    close_extra_tabs(driver)
    try:
        driver.get(search_url)
    except Exception as e:
        log.warning(f"  driver.get failed ({e}), retrying with JS navigation...")
        close_extra_tabs(driver)
        try:
            driver.execute_script(f"window.location.href = '{search_url}';")
        except Exception as e2:
            log.error(f"  JS navigation also failed: {e2}")
            raise
    time.sleep(4)

    # Check for IP-level search throttle immediately after page load
    if check_search_throttle(driver):
        log.warning("  SEARCH THROTTLED — IP temporarily paused by newspapers.com.")
        log.warning("  Setting global stop flag — all collector instances will stop.")
        GLOBAL_STOP_FLAG.parent.mkdir(parents=True, exist_ok=True)
        GLOBAL_STOP_FLAG.mkdir(exist_ok=True)
        return -1, start_date

    seen_urls = set()
    new_entries = 0
    skipped_exists = 0
    skipped_no_date = 0
    latest_date = start_date
    no_new_rounds = 0
    zero_new_batches = 0

    while True:
        if check_stop():
            log.info("  Stop flag detected.")
            break

        # Check for IP-level throttle mid-session
        if check_search_throttle(driver):
            log.warning("  SEARCH THROTTLED mid-session — stopping all collectors.")
            GLOBAL_STOP_FLAG.parent.mkdir(parents=True, exist_ok=True)
            GLOBAL_STOP_FLAG.mkdir(exist_ok=True)
            return new_entries, latest_date

        results = scrape_results(driver)
        new_results = [r for r in results if r["url"] not in seen_urls]

        if not new_results:
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

        new_metas = []
        for item in new_results:
            seen_urls.add(item["url"])
            meta = parse_result(item)

            if not meta["date_str"]:
                skipped_no_date += 1
                log.debug(f"    SKIP (no date): {meta.get('url', '?')}")
                continue

            if meta["date_str"] > latest_date:
                latest_date = meta["date_str"]

            if entry_exists(conn, meta["pdf_filename"]):
                update_url(conn, meta["pdf_filename"], meta["url"])
                skipped_exists += 1
                continue

            new_metas.append(meta)

        # Download thumbnails for new entries
        thumb_paths = {}
        if new_metas:
            thumb_paths = download_thumbnails(driver, new_metas)

        for meta in new_metas:
            # Check account daily URL limit before each insert
            if max_urls and gathered_by and not is_collector_account_eligible(conn, gathered_by, max_urls):
                log.info(
                    f"  Account daily limit reached for {gathered_by} (max {max_urls}/day)"
                )
                # Update stats for what we inserted so far this batch
                if batch_new > 0:
                    update_collector_account_stats(gathered_by, urls_added=batch_new)
                return new_entries, latest_date

            thumb = thumb_paths.get(meta["pdf_filename"])
            if create_entry(conn, meta, thumbnail_path=thumb, gathered_by=gathered_by):
                new_entries += 1
                batch_new += 1
            else:
                skipped_exists += 1

            if new_entries % 50 == 0:
                write_status(
                    current_date=latest_date,
                    status="running",
                )
                if slot_id:
                    write_instance_status(
                        slot_id,
                        urls_this_run=total_new_before + new_entries,
                        current_date=latest_date,
                        last_action=f"+{new_entries} URLs at {latest_date}",
                    )
                log.info(
                    f"  Progress: {new_entries} new, "
                    f"{skipped_exists} already in DB, "
                    f"{skipped_no_date} no date, "
                    f"at {latest_date}"
                )

            if restart_every and new_entries % restart_every == 0:
                log.info(
                    f"  Restart threshold ({restart_every}) — "
                    f"restarting from {latest_date}"
                )
                return new_entries, latest_date

        log.info(
            f"  Batch: {batch_new} new, {len(new_results) - batch_new} skipped "
            f"({skipped_exists} in DB, {skipped_no_date} no date)"
        )

        # Update daily account stats for this batch
        if batch_new > 0 and gathered_by:
            update_collector_account_stats(gathered_by, urls_added=batch_new)

        # Throttle detection
        if batch_new == 0 and len(new_results) > 0:
            zero_new_batches += 1
            if zero_new_batches >= 3:
                log.info(
                    f"  All results already in DB ({zero_new_batches} consecutive "
                    f"batches with 0 new). Moving on."
                )
                break
        else:
            zero_new_batches = 0

        scroll_for_more(driver, len(results))

    return new_entries, latest_date


# === MAIN ===

def main_slot(slot_id, date_start, date_end, max_urls=1500, restart_every=100):
    """Multi-instance worker entry point.

    Uses browser_session for account management. Each slot gets its own
    Chrome profile and collector_instances row. Exits when date range is
    exhausted or a stop flag is set.
    """
    pid = os.getpid()
    started_at = _now_str()
    profile_dir = str(RUNTIME_DIR / f"chrome_temp_profile_collector_{slot_id}")

    log.info("=" * 60)
    log.info(f"URL Collector — slot {slot_id}")
    log.info("=" * 60)
    log.info(f"  Date range: {date_start} to {date_end}")
    log.info(f"  Max URLs per account: {max_urls}")
    log.info(f"  Restart every: {restart_every}")
    log.info(f"  Profile: {profile_dir}")

    write_instance_status(
        slot_id,
        pid=pid,
        status="starting",
        date_start=date_start,
        date_end=date_end,
        urls_this_run=0,
        started_at=started_at,
        last_action="starting",
    )

    session = BrowserSession(db_path=DB_PATH, profile_dir=profile_dir, app='collector')
    conn = get_db()
    total_new = 0
    session_num = 0
    consecutive_failures = 0
    driver = None
    account_urls = 0
    force_start = None

    try:
        while True:
            if check_instance_stop(slot_id):
                log.info("Stop flag set — exiting.")
                break

            session_num += 1
            resume_date = get_resume_date(conn, date_start)
            if force_start and force_start > resume_date:
                log.info(f"  Forcing start from {force_start} (was {resume_date})")
                resume_date = force_start
            if resume_date > date_end:
                log.info(f"  Past end date ({date_end}). Done.")
                break

            log.info(f"\n>>> SESSION {session_num} (from {resume_date})")

            if driver is None:
                try:
                    write_instance_status(slot_id, status="logging-in",
                                          last_action="setting up browser")
                    # Build exclude list: accounts that hit today's daily URL limit
                    _exclude = get_ineligible_collector_emails(conn, max_urls)
                    if _exclude:
                        log.info(f"  Excluding accounts over daily limit ({max_urls}): {_exclude}")
                    driver = session.setup_driver(
                        preferred_account=None, exclude_emails=_exclude,
                    )
                    if driver is None or not session.current_email:
                        log.error("  No account logged in. Stopping.")
                        driver = None
                        break
                    # Initialize from DB so we know how many this account already gathered
                    account_urls = conn.execute(
                        "SELECT COUNT(*) FROM processed_pdfs WHERE gathered_by = ?",
                        (session.current_email,),
                    ).fetchone()[0]
                    write_instance_status(
                        slot_id,
                        status="running",
                        account_email=session.current_email,
                        last_action=f"logged in as {session.current_email} ({account_urls} existing)",
                    )
                    log.info(f"  Logged in as: {session.current_email}")
                    if account_urls:
                        log.info(f"  Account already has {account_urls} URLs in DB")
                    consecutive_failures = 0
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
                new, latest = run_session(
                    conn, driver, resume_date, date_end, restart_every,
                    gathered_by=session.current_email or "",
                    max_urls=max_urls, account_urls_so_far=account_urls,
                    slot_id=slot_id, total_new_before=total_new,
                )
                if new == -1:
                    log.warning("  Search throttled — exiting slot.")
                    write_instance_status(
                        slot_id, status="throttled",
                        last_action="IP search throttle detected",
                    )
                    break
                total_new += new
                account_urls += new

                write_instance_status(
                    slot_id,
                    status="running",
                    urls_this_run=total_new,
                    current_date=latest,
                    account_email=session.current_email,
                    last_action=f"+{new} URLs at {latest}",
                )

                log.info(
                    f"  Session {session_num}: {new} new "
                    f"(total: {total_new}, account: {account_urls}, "
                    f"at {latest})"
                )

                if max_urls and not is_collector_account_eligible(conn, session.current_email, max_urls):
                    log.info(
                        f"  ACCOUNT DAILY LIMIT: {session.current_email} hit "
                        f"{max_urls} URLs today."
                    )
                    write_instance_status(slot_id, last_action="rotating account")
                    log.info("  Rotating to next account — quitting browser for clean restart...")
                    # Quit browser and release account — loop will claim next and launch fresh
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                    continue

                if new == 0:
                    try:
                        next_dt = datetime.strptime(
                            latest, "%Y-%m-%d"
                        ) + timedelta(days=1)
                        next_date = next_dt.strftime("%Y-%m-%d")
                        if next_date <= date_end:
                            log.info(
                                f"  No new results at {latest}. "
                                f"Advancing to {next_date}."
                            )
                            force_start = next_date
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
                    force_start = None

                if new > 0:
                    log.info("  Continuing with same browser session...")
                    time.sleep(2)

            except Exception as e:
                log.error(f"  Session error: {e}", exc_info=True)
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
                log.info("  Restarting browser in 3 seconds...")
                time.sleep(3)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    finally:
        write_instance_status(slot_id, status="stopped",
                              urls_this_run=total_new, last_action="exiting")
        session.shutdown(driver)
        conn.close()
        delete_instance_row(slot_id)

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Sessions: {session_num}")
    log.info(f"  Total new URLs: {total_new}")
    log.info("=" * 60)


def main():
    parser = argparse.ArgumentParser(
        description="Collect search result URLs from newspapers.com"
    )
    parser.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date YYYY-MM-DD")
    parser.add_argument(
        "--account", default="",
        help="Preferred account email (auto-select if not given)",
    )
    parser.add_argument(
        "--restart-every", type=int, default=100,
        help="Restart search from latest date every N new entries",
    )
    parser.add_argument(
        "--max-urls", type=int, default=1500,
        help="Max new URLs per account before rotating",
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
    log.info("URL Collector (v2 — browser_session)")
    log.info("=" * 60)
    log.info(f"  Date range: {args.start} to {args.end}")
    log.info(f"  Preferred account: {args.account or '(auto)'}")
    log.info(f"  Restart every: {args.restart_every}")
    log.info(f"  Max URLs per account: {args.max_urls}")
    log.info(f"  Log: {log_filename}")

    write_status(
        status="running",
        pid=os.getpid(),
        account="(starting)",
        date_start=args.start,
        date_end=args.end,
        restart_every=args.restart_every,
        new_entries=0,
        current_date=args.start,
    )

    session = BrowserSession(db_path=DB_PATH, profile_dir=str(PROFILE_DIR), app='collector')
    conn = get_db()
    total_new = 0
    session_num = 0
    consecutive_failures = 0
    driver = None
    account_urls = 0  # URLs collected on current account
    force_start = None  # Override resume date when stuck on a fully-collected date

    try:
        while True:
            if check_stop():
                log.info("Stop flag set — exiting.")
                break

            session_num += 1
            resume_date = get_resume_date(conn, args.start)
            if force_start and force_start > resume_date:
                log.info(f"  Forcing start from {force_start} (was {resume_date})")
                resume_date = force_start
            if resume_date > args.end:
                log.info(f"  Past end date ({args.end}). Done.")
                break

            log.info(f"\n>>> SESSION {session_num} (from {resume_date})")

            # Create browser and login if needed
            if driver is None:
                try:
                    preferred = args.account or None
                    # Build exclude list: accounts that hit today's daily URL limit
                    _exclude = get_ineligible_collector_emails(conn, args.max_urls)
                    if _exclude:
                        log.info(f"  Excluding accounts over daily limit ({args.max_urls}): {_exclude}")
                    driver = session.setup_driver(
                        preferred_account=preferred, exclude_emails=_exclude,
                    )
                    if driver is None or not session.current_email:
                        log.error("  No account logged in. Stopping.")
                        driver = None
                        break
                    write_status(account=session.current_email)
                    log.info(f"  Logged in as: {session.current_email}")
                    account_urls = conn.execute(
                        "SELECT COUNT(*) FROM processed_pdfs WHERE gathered_by = ?",
                        (session.current_email,),
                    ).fetchone()[0]
                    if account_urls:
                        log.info(f"  Account already has {account_urls} URLs in DB")
                    consecutive_failures = 0
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
                new, latest = run_session(
                    conn, driver, resume_date, args.end, args.restart_every,
                    gathered_by=session.current_email or "",
                    max_urls=args.max_urls, account_urls_so_far=account_urls,
                )
                if new == -1:
                    log.warning("  Search throttled — stopping.")
                    write_status(status="throttled")
                    break
                total_new += new
                account_urls += new

                write_status(
                    new_entries=total_new,
                    current_date=latest,
                    account=session.current_email,
                )
                log.info(
                    f"  Session {session_num}: {new} new "
                    f"(total: {total_new}, account: {account_urls}, "
                    f"at {latest})"
                )

                # Check per-account daily URL limit
                if args.max_urls and not is_collector_account_eligible(conn, session.current_email, args.max_urls):
                    log.info(
                        f"  ACCOUNT DAILY LIMIT: {session.current_email} hit "
                        f"{args.max_urls} URLs today."
                    )
                    log.info("  Rotating to next account — quitting browser for clean restart...")
                    if driver:
                        try:
                            driver.quit()
                        except Exception:
                            pass
                        driver = None
                    continue

                if new == 0:
                    try:
                        next_dt = datetime.strptime(
                            latest, "%Y-%m-%d"
                        ) + timedelta(days=1)
                        next_date = next_dt.strftime("%Y-%m-%d")
                        if next_date <= args.end:
                            log.info(
                                f"  No new results at {latest}. "
                                f"Advancing to {next_date}."
                            )
                            force_start = next_date
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
                    force_start = None

                # Continue with same browser session
                if new > 0:
                    log.info("  Continuing with same browser session...")
                    time.sleep(2)

            except Exception as e:
                log.error(f"  Session error: {e}", exc_info=True)
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
                log.info("  Restarting browser in 3 seconds...")
                time.sleep(3)

    except KeyboardInterrupt:
        log.info("\nStopped by user.")
    finally:
        write_status(status="stopped", new_entries=total_new)
        session.shutdown(driver)
        conn.close()

    log.info("=" * 60)
    log.info("DONE")
    log.info(f"  Sessions: {session_num}")
    log.info(f"  Total new URLs: {total_new}")
    log.info("=" * 60)


if __name__ == "__main__":
    import sys

    # Extract --slot-id=X from argv without disturbing argparse.
    _slot_id = None
    _filtered = [sys.argv[0]]
    _skip_next = False
    for _i, _a in enumerate(sys.argv[1:], 1):
        if _skip_next:
            _skip_next = False
            continue
        if _a.startswith("--slot-id="):
            _slot_id = _a.split("=", 1)[1].strip() or None
        elif _a == "--slot-id" and _i < len(sys.argv) - 1:
            _slot_id = sys.argv[_i + 1].strip()
            _skip_next = True
        else:
            _filtered.append(_a)
    sys.argv = _filtered

    if _slot_id:
        # Retag the logger with the slot id so each instance has its own log.
        _slot_log = os.path.join(
            LOG_DIR, f"collector_slot{_slot_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        _fh = logging.FileHandler(_slot_log)
        _fh.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        for _h in log.handlers[:]:
            if isinstance(_h, logging.FileHandler) and _h is not _fh:
                log.removeHandler(_h)
        log.addHandler(_fh)
        log.info(f"  Slot log file: {_slot_log}")

        # Parse remaining args for date range and limits
        import argparse as _ap
        _p = _ap.ArgumentParser()
        _p.add_argument("--start", default="1900-01-01")
        _p.add_argument("--end", default="2100-12-31")
        _p.add_argument("--restart-every", type=int, default=100)
        _p.add_argument("--max-urls", type=int, default=1500)
        _args = _p.parse_args()
        main_slot(_slot_id, _args.start, _args.end,
                  max_urls=_args.max_urls, restart_every=_args.restart_every)
    else:
        main()
