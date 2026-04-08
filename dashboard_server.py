"""
Simple HTTP server that serves the dashboard and provides API endpoints
to read from the SQLite database. Auto-refreshes as extraction runs.

Usage: python dashboard_server.py
Then open http://localhost:8765 in your browser.
"""

import json
import re
import sqlite3
import os
import glob
import subprocess
import signal
import urllib.request
import urllib.error
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

from datetime import datetime

BASE_DIR = Path(r"C:\lake_worth")
DB_PATH = BASE_DIR / "lake_worth.db"
PDF_DIR = BASE_DIR / "pdfs"
LOG_DIR = BASE_DIR / "collector_logs"
STOP_FLAG = BASE_DIR / "stop_clipper"
PORT = 8765


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


def _is_clipper_running():
    """Check if clip_and_extract.py is running."""
    try:
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "CommandLine"],
            capture_output=True, text=True, timeout=5
        )
        return "clip_and_extract" in result.stdout
    except Exception:
        return False


def ensure_accounts_table():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            label TEXT DEFAULT '',
            subscription_type TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            total_clips INTEGER DEFAULT 0,
            clips_this_session INTEGER DEFAULT 0,
            last_clip_time TEXT,
            last_throttle_time TEXT,
            throttle_count INTEGER DEFAULT 0,
            avg_clips_before_throttle REAL DEFAULT 0,
            last_login_time TEXT,
            last_logout_time TEXT,
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()
    conn.close()


ensure_accounts_table()


def get_dashboard_data():
    conn = get_db()

    # Stats
    total_pdfs = len(list(PDF_DIR.glob("*.pdf")))
    processed = conn.execute("SELECT COUNT(*) as c FROM processed_pdfs").fetchone()["c"]
    articles = conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()["c"]
    quotes = conn.execute("SELECT COUNT(*) as c FROM quotes").fetchone()["c"]
    people = conn.execute("SELECT COUNT(*) as c FROM people").fetchone()["c"]
    images = conn.execute("SELECT COUNT(*) as c FROM images").fetchone()["c"]

    stats = {
        "total_pdfs": total_pdfs,
        "processed": processed,
        "articles": articles,
        "quotes": quotes,
        "people": people,
        "images": images,
    }

    # Articles with their quotes
    article_rows = conn.execute(
        "SELECT a.*, pp.url AS page_url, pp.clip_url FROM articles a LEFT JOIN processed_pdfs pp ON a.pdf_filename = pp.pdf_filename ORDER BY a.date, a.page"
    ).fetchall()

    article_list = []
    for a in article_rows:
        a_dict = dict(a)
        a_quotes = conn.execute(
            "SELECT * FROM quotes WHERE article_id = ?", (a["id"],)
        ).fetchall()
        a_dict["quotes"] = [dict(q) for q in a_quotes]
        # Attach clip image if available
        img_row = conn.execute(
            "SELECT cropped_image_file FROM images WHERE article_id = ?", (a["id"],)
        ).fetchone()
        if img_row:
            img_file = img_row["cropped_image_file"]
            if img_file.startswith("clip_"):
                a_dict["clip_image"] = img_file
            else:
                a_dict["clip_image"] = img_file
        article_list.append(a_dict)

    # All quotes with article info
    quote_rows = conn.execute("""
        SELECT q.*, a.headline, a.date, a.newspaper
        FROM quotes q
        JOIN articles a ON q.article_id = a.id
        ORDER BY a.date
    """).fetchall()
    quote_list = [dict(q) for q in quote_rows]

    # People
    people_rows = conn.execute(
        "SELECT * FROM people ORDER BY first_seen_date"
    ).fetchall()
    people_list = [dict(p) for p in people_rows]

    # Processing log (from processed_pdfs + article counts)
    log_rows = conn.execute("""
        SELECT
            pp.pdf_filename,
            pp.processed_at,
            COUNT(a.id) as article_count
        FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        GROUP BY pp.pdf_filename
        ORDER BY pp.processed_at
    """).fetchall()

    log_list = []
    for row in log_rows:
        fname = row["pdf_filename"]
        # Parse filename for display
        import re
        m = re.match(
            r"^(?P<paper>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<page>\d+)\.pdf$",
            fname, re.IGNORECASE
        )
        if m:
            newspaper = m.group("paper").replace("_", " ")
            date_str = f"{m.group('year')}-{m.group('month')}-{m.group('day')}"
            page = int(m.group("page"))
        else:
            newspaper = fname
            date_str = "?"
            page = 0

        log_list.append({
            "filename": fname,
            "date": date_str,
            "page": page,
            "newspaper": newspaper,
            "articles": row["article_count"],
            "status": "OK" if row["article_count"] > 0 else "ok(0)",
        })

    # No-articles count (for stats)
    no_articles_count = conn.execute("""
        SELECT COUNT(*) as c FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        AND (pp.ignored IS NULL OR pp.ignored = 0)
    """).fetchone()["c"]

    # No-articles filenames for aggregation
    na_fnames = conn.execute("""
        SELECT pp.pdf_filename FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        AND (pp.ignored IS NULL OR pp.ignored = 0)
    """).fetchall()

    # Parse dates from filenames for monthly/yearly counts
    na_monthly = {}
    na_yearly = {}
    for row in na_fnames:
        m = re.search(r'_(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', row["pdf_filename"])
        if m:
            ym = f"{m.group(1)}-{m.group(2)}"
            y = m.group(1)
            na_monthly[ym] = na_monthly.get(ym, 0) + 1
            na_yearly[y] = na_yearly.get(y, 0) + 1

    # Monthly reference counts
    monthly_rows = conn.execute("""
        SELECT substr(date, 1, 7) as ym, COUNT(*) as c FROM articles
        WHERE date IS NOT NULL AND length(date) >= 7
        GROUP BY ym
    """).fetchall()
    monthly = {r["ym"]: r["c"] for r in monthly_rows}
    for ym, c in na_monthly.items():
        monthly[ym] = monthly.get(ym, 0) + c
    monthly_sorted = sorted(monthly.items())

    # Articles by year
    articles_by_year_rows = conn.execute("""
        SELECT substr(date, 1, 4) as y, COUNT(*) as c FROM articles
        WHERE date IS NOT NULL AND length(date) >= 4
        GROUP BY y ORDER BY y
    """).fetchall()
    articles_by_year_sorted = [(r["y"], r["c"]) for r in articles_by_year_rows]

    # No articles by year
    no_articles_by_year_sorted = sorted(na_yearly.items())

    conn.close()

    return {
        "stats": stats,
        "articles": article_list,
        "quotes": quote_list,
        "people": people_list,
        "log": log_list,
        "no_articles_count": no_articles_count,
        "monthly": monthly_sorted,
        "articles_by_year": articles_by_year_sorted,
        "no_articles_by_year": no_articles_by_year_sorted,
    }


def get_no_articles_page(page=1, per_page=100, sort="desc", filter_text=""):
    """Return one page of no-articles entries, sorted by date."""
    conn = get_db()
    order = "DESC" if sort == "desc" else "ASC"
    offset = (page - 1) * per_page

    # Count total (with optional filter)
    if filter_text:
        like = f"%{filter_text}%"
        total = conn.execute("""
            SELECT COUNT(*) as c FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            AND (pp.ignored IS NULL OR pp.ignored = 0)
            AND pp.pdf_filename LIKE ?
        """, (like,)).fetchone()["c"]
        rows = conn.execute(f"""
            SELECT pp.pdf_filename, pp.search_term, pp.url, pp.clip_url,
                   pp.thumbnail_path, pp.ignored, pp.highlighted, pp.has_photo
            FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            AND (pp.ignored IS NULL OR pp.ignored = 0)
            AND pp.pdf_filename LIKE ?
            ORDER BY pp.date_str {order}, pp.pdf_filename {order}
            LIMIT ? OFFSET ?
        """, (like, per_page, offset)).fetchall()
    else:
        total = conn.execute("""
            SELECT COUNT(*) as c FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            AND (pp.ignored IS NULL OR pp.ignored = 0)
        """).fetchone()["c"]
        rows = conn.execute(f"""
            SELECT pp.pdf_filename, pp.search_term, pp.url, pp.clip_url,
                   pp.thumbnail_path, pp.ignored, pp.highlighted, pp.has_photo
            FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            AND (pp.ignored IS NULL OR pp.ignored = 0)
            ORDER BY pp.date_str {order}, pp.pdf_filename {order}
            LIMIT ? OFFSET ?
        """, (per_page, offset)).fetchall()

    items = []
    for row in rows:
        fname = row["pdf_filename"]
        m = re.match(
            r"^(?P<paper>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<page>\d+)\.pdf$",
            fname, re.IGNORECASE
        )
        if m:
            newspaper = m.group("paper").replace("_", " ")
            date_str = f"{m.group('year')}-{m.group('month')}-{m.group('day')}"
            pg = int(m.group("page"))
        else:
            newspaper = fname
            date_str = "?"
            pg = 0

        items.append({
            "filename": fname,
            "date": date_str,
            "page": pg,
            "newspaper": newspaper,
            "search_term": row["search_term"],
            "url": row["url"] or "",
            "clip_url": row["clip_url"] or "",
            "thumbnail": row["thumbnail_path"] or "",
            "ignored": row["ignored"] or 0,
            "highlighted": row["highlighted"] or 0,
            "has_photo": row["has_photo"] or 0,
        })

    conn.close()
    total_pages = (total + per_page - 1) // per_page
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
    }


def _extract_ld_json(html: str):
    """Extract JSON-LD block from HTML. Returns dict or None."""
    ld_match = re.search(r'<script[^>]*type="application/ld\+json"[^>]*>(.*?)</script>', html, re.DOTALL)
    if ld_match:
        try:
            return json.loads(ld_match.group(1))
        except (json.JSONDecodeError, ValueError):
            pass
    return None


def _fetch_with_chrome(url: str) -> str:
    """Disabled — was launching stray Chrome browsers via undetected_chromedriver."""
    raise RuntimeError("Chrome fetch disabled — use direct HTTP instead")


def fetch_clip(url: str) -> dict:
    """Fetch a newspapers.com clip URL and extract OCR text + metadata.

    First tries a simple HTTP fetch. If the page lacks JSON-LD data
    (authentication required) or returns 403, falls back to headless
    Chrome with the saved login profile.
    """
    html = None
    need_chrome = False

    # Try simple fetch first
    try:
        req = urllib.request.Request(url, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml",
        })
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode("utf-8", errors="replace")
        # Check if we got useful data
        if not _extract_ld_json(html):
            need_chrome = True
    except urllib.error.HTTPError as e:
        if e.code == 403:
            need_chrome = True
        else:
            raise

    if need_chrome:
        html = _fetch_with_chrome(url)

    result = {}

    # Extract JSON-LD metadata
    ld = _extract_ld_json(html)
    if ld:
        result["date"] = ld.get("datePublished", "")
        result["page"] = int(ld.get("pageStart", 0) or 0)

        pub = ld.get("publisher", "")
        if isinstance(pub, dict):
            result["newspaper"] = pub.get("legalName", "") or pub.get("name", "")
        else:
            result["newspaper"] = str(pub)

        loc = ld.get("locationCreated", "")
        if isinstance(loc, dict):
            result["location"] = loc.get("name", "")
        else:
            result["location"] = str(loc)

        ocr = ld.get("text", "")
        if ocr:
            result["ocr_text"] = ocr

        headline = ld.get("headline", "")
        if not headline and ocr:
            caps_match = re.search(r'([A-Z][A-Z\s]{8,}[A-Z])', ocr)
            if caps_match:
                headline = caps_match.group(1).strip()
        result["headline"] = headline

    # Fallback: try og:description or meta description
    if not result.get("ocr_text"):
        og_desc = re.search(r'<meta\s+property="og:description"\s+content="([^"]*)"', html)
        if og_desc:
            result["ocr_text"] = og_desc.group(1)
    if not result.get("ocr_text"):
        meta_desc = re.search(r'<meta\s+name="description"\s+content="([^"]*)"', html)
        if meta_desc:
            result["ocr_text"] = meta_desc.group(1)

    # Extract clipping ID from URL
    clip_match = re.search(r'/(\d+)/?$', url)
    if clip_match:
        result["clip_id"] = clip_match.group(1)

    # Extract image URL from the page
    # Look for clipping image: img.newspapers.com/img/img?id=...&clippingId=...
    img_match = re.search(r'(https://img\.newspapers\.com/img/img\?id=\d+&amp;clippingId=\d+[^"\']*)', html)
    if not img_match:
        img_match = re.search(r'(https://img\.newspapers\.com/img/img\?id=\d+&clippingId=\d+[^"\']*)', html)
    if not img_match:
        # Try thumbnail URL and convert
        thumb_match = re.search(r'(https://img\.newspapers\.com/img/thumbnail/(\d+)/[^"\']*)', html)
        if thumb_match and result.get("clip_id"):
            image_id = thumb_match.group(2)
            result["image_url"] = f"https://img.newspapers.com/img/img?id={image_id}&clippingId={result['clip_id']}&width=1200&height=1200"
    if img_match:
        result["image_url"] = img_match.group(1).replace("&amp;", "&")

    result["source_url"] = url
    return result


def _make_working_title(text: str) -> str:
    """Generate 'Untitled - [AI title]' using Claude to summarize the OCR text."""
    if not text:
        return "Untitled"
    clean = text.replace("&apos;", "'").replace("&quot;", '"').replace("&amp;", "&")
    try:
        import anthropic
        client = anthropic.Anthropic()
        resp = client.messages.create(
            model="claude-haiku-4-5-20251001",
            max_tokens=30,
            messages=[{"role": "user", "content":
                f"Generate a short newspaper headline (3-8 words, title case) for this article excerpt. "
                f"Reply with ONLY the headline, nothing else.\n\n{clean[:500]}"}],
        )
        title = resp.content[0].text.strip().strip('"\'')
        if title:
            return f"Untitled - {title}"
    except Exception:
        pass
    # Fallback: first few words
    words = clean.split()[:6]
    snippet = " ".join(words)
    if len(snippet) > 40:
        snippet = snippet[:37] + "..."
    return f"Untitled - {snippet}"


def _fuzzy_match(text_a: str, text_b: str) -> float:
    """Quick word-overlap ratio between two texts (0.0 to 1.0)."""
    if not text_a or not text_b:
        return 0.0
    words_a = set(re.findall(r'[a-z]{3,}', text_a.lower()))
    words_b = set(re.findall(r'[a-z]{3,}', text_b.lower()))
    if not words_a or not words_b:
        return 0.0
    overlap = words_a & words_b
    return len(overlap) / min(len(words_a), len(words_b))


def _download_clip_image(clip_data: dict, date: str, page: int) -> str:
    """Download clip image, return filename or empty string."""
    image_url = clip_data.get("image_url", "")
    if not image_url:
        return ""
    try:
        clip_dir = BASE_DIR / "clip_images"
        clip_dir.mkdir(exist_ok=True)
        clip_id = clip_data.get("clip_id", "")
        img_filename = f"clip_{date}_{page}_{clip_id}.jpg"
        img_path = clip_dir / img_filename
        img_req = urllib.request.Request(image_url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(img_req, timeout=15) as img_resp:
            with open(img_path, "wb") as f:
                f.write(img_resp.read())
        return img_filename
    except Exception as e:
        return f"error: {e}"


def import_clip(clip_data: dict, replace_article_id: int = None) -> dict:
    """Insert or update an article from clip data.

    If an existing article on the same date+page is found, upgrades it
    with the cleaner clip OCR text and image. For pages with multiple
    articles, uses fuzzy text matching to find the right one.
    """
    conn = get_db()

    date = clip_data.get("date", "")
    newspaper = clip_data.get("newspaper", "")
    page = clip_data.get("page", 0)
    headline = clip_data.get("headline", "")
    ocr_text = clip_data.get("ocr_text", "")
    source_url = clip_data.get("source_url", "")
    clip_id = clip_data.get("clip_id", "")

    # Duplicate check: reject if this clip_id already exists
    if clip_id and not replace_article_id:
        existing = conn.execute(
            "SELECT id, headline FROM articles WHERE clip_id = ?", (clip_id,)
        ).fetchone()
        if existing:
            conn.close()
            return {"error": f"Duplicate — clip already imported as article #{existing['id']}: {existing['headline'] or 'Untitled'}"}

    # Download clip image
    clip_image_file = _download_clip_image(clip_data, date, page)
    has_image = 1 if clip_image_file and not clip_image_file.startswith("error") else 0

    # Find matching PDF filename for this date/page
    pdf_pattern = f"%_{date.replace('-', '_')}_{page}.pdf"
    pdf_row = conn.execute(
        "SELECT pdf_filename FROM processed_pdfs WHERE pdf_filename LIKE ?",
        (pdf_pattern,)
    ).fetchone()
    pdf_filename = pdf_row["pdf_filename"] if pdf_row else ""

    # Look up search term
    search_term = "lake worth"
    if pdf_row:
        st_row = conn.execute(
            "SELECT search_term FROM processed_pdfs WHERE pdf_filename = ?",
            (pdf_filename,)
        ).fetchone()
        if st_row and st_row["search_term"]:
            search_term = st_row["search_term"]

    # Try to match an existing article to upgrade
    match_id = replace_article_id
    action = "updated"

    if not match_id:
        existing = conn.execute(
            "SELECT id, full_text, headline, clip_id FROM articles WHERE date = ? AND page = ?",
            (date, page)
        ).fetchall()

        if len(existing) == 1 and not existing[0]["clip_id"]:
            # Single article on this page without a clip — upgrade it
            match_id = existing[0]["id"]
        elif len(existing) > 1:
            # Multiple articles — fuzzy match to find the right one
            best_id, best_score = None, 0.3  # minimum threshold
            for row in existing:
                if row["clip_id"]:
                    continue  # already has a clip, skip
                score = _fuzzy_match(ocr_text, row["full_text"])
                if score > best_score:
                    best_score = score
                    best_id = row["id"]
            match_id = best_id

    if match_id:
        # Upgrade existing article with cleaner clip data
        old = conn.execute("SELECT headline FROM articles WHERE id = ?", (match_id,)).fetchone()
        # Keep old headline if clip doesn't have one; generate working title if neither has one
        use_headline = headline if headline else (old["headline"] if old else "")
        if not use_headline:
            use_headline = _make_working_title(ocr_text)
        # Inherit page-level flags set by user on the "no articles" tab (promote only)
        pdf_row = conn.execute(
            "SELECT highlighted, has_photo FROM processed_pdfs WHERE pdf_filename = ?",
            (pdf_filename,)
        ).fetchone() if pdf_filename else None
        pdf_highlighted = (pdf_row["highlighted"] or 0) if pdf_row else 0
        pdf_has_photo = (pdf_row["has_photo"] or 0) if pdf_row else 0
        conn.execute(
            """UPDATE articles SET full_text = ?, headline = ?, has_image = ?,
                                   clip_id = ?, newspaper = ?,
                                   highlighted = COALESCE(NULLIF(highlighted,0), ?),
                                   has_photo   = COALESCE(NULLIF(has_photo,0), ?)
               WHERE id = ?""",
            (ocr_text, use_headline, has_image, clip_id or None, newspaper, pdf_highlighted, pdf_has_photo, match_id),
        )
        article_id = match_id

        # Replace old image with clip image
        if clip_image_file and not clip_image_file.startswith("error"):
            conn.execute("DELETE FROM images WHERE article_id = ?", (match_id,))
            conn.execute(
                """INSERT INTO images (cropped_image_file, caption, description, article_id, pdf_filename)
                   VALUES (?, ?, ?, ?, ?)""",
                (clip_image_file, use_headline, "Clip from newspapers.com", match_id, pdf_filename),
            )

        conn.commit()
        conn.close()
        return {"action": "updated", "article_id": match_id, "pdf_filename": pdf_filename,
                "clip_image": clip_image_file or ""}
    else:
        # No match — create new article
        action = "created"
        if not headline:
            headline = _make_working_title(ocr_text)
        # Inherit page-level flags set by user on the "no articles" tab
        pdf_row = conn.execute(
            "SELECT highlighted, has_photo FROM processed_pdfs WHERE pdf_filename = ?",
            (pdf_filename,)
        ).fetchone() if pdf_filename else None
        pdf_highlighted = (pdf_row["highlighted"] or 0) if pdf_row else 0
        pdf_has_photo = (pdf_row["has_photo"] or 0) if pdf_row else 0
        cur = conn.execute(
            """INSERT INTO articles (date, newspaper, page, headline, full_text,
                                     pdf_filename, has_image, search_term, clip_id, highlighted, has_photo)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (date, newspaper, page, headline, ocr_text, pdf_filename, has_image, search_term, clip_id or None, pdf_highlighted, pdf_has_photo),
        )
        article_id = cur.lastrowid

        if clip_image_file and not clip_image_file.startswith("error"):
            conn.execute(
                """INSERT INTO images (cropped_image_file, caption, description, article_id, pdf_filename)
                   VALUES (?, ?, ?, ?, ?)""",
                (clip_image_file, headline, "Clip from newspapers.com", article_id, pdf_filename),
            )

        # Update processed_pdfs count
        if pdf_filename:
            count = conn.execute(
                "SELECT COUNT(*) as c FROM articles WHERE pdf_filename = ?",
                (pdf_filename,)
            ).fetchone()["c"]
            conn.execute(
                "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
                (count, pdf_filename),
            )

        conn.commit()
        conn.close()
        return {"action": "created", "article_id": article_id, "pdf_filename": pdf_filename,
                "clip_image": clip_image_file}


class DashboardHandler(SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=str(BASE_DIR), **kwargs)

    def do_GET(self):
        if self.path == "/" or self.path == "/index.html":
            self.send_response(200)
            self.send_header("Content-type", "text/html")
            self.end_headers()
            with open(BASE_DIR / "dashboard.html", "rb") as f:
                self.wfile.write(f.read())
        elif self.path == "/api/data":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                data = get_dashboard_data()
                self.wfile.write(json.dumps(data, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/no-articles"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                from urllib.parse import urlparse, parse_qs
                params = parse_qs(urlparse(self.path).query)
                page = int(params.get("page", [1])[0])
                per_page = int(params.get("per_page", [100])[0])
                sort = params.get("sort", ["desc"])[0]
                filter_text = params.get("filter", [""])[0]
                data = get_no_articles_page(page, per_page, sort, filter_text)
                self.wfile.write(json.dumps(data, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/db-viewer":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                tables = {}
                for tbl in conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall():
                    name = tbl["name"]
                    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({name})").fetchall()]
                    count = conn.execute(f"SELECT COUNT(*) as c FROM {name}").fetchone()["c"]
                    # Get recent rows (limit 100, most recent first)
                    rows = conn.execute(f"SELECT * FROM {name} ORDER BY rowid DESC LIMIT 100").fetchall()
                    tables[name] = {
                        "columns": cols,
                        "count": count,
                        "rows": [dict(r) for r in rows],
                    }
                conn.close()
                self.wfile.write(json.dumps(tables, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/screenshots":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import glob as g
                screenshots = sorted(
                    g.glob(str(LOG_DIR / "screenshot_*.png")),
                    key=os.path.getmtime, reverse=True
                )
                files = []
                for path in screenshots:
                    name = os.path.basename(path)
                    mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
                    files.append({"name": name, "time": mtime})
                self.wfile.write(json.dumps(files).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/screenshot/"):
            filename = self.path.split("/api/screenshot/")[1]
            filepath = LOG_DIR / filename
            if filepath.exists() and filepath.suffix == ".png" and ".." not in filename:
                self.send_response(200)
                self.send_header("Content-type", "image/png")
                self.end_headers()
                with open(filepath, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()
        elif self.path == "/api/accounts":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                rows = conn.execute("SELECT * FROM accounts ORDER BY active DESC, total_clips DESC").fetchall()
                accounts = [dict(r) for r in rows]
                conn.close()
                self.wfile.write(json.dumps(accounts, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/progress":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                log_files = sorted(glob.glob(str(LOG_DIR / "clipper_*.log")), key=os.path.getmtime, reverse=True)
                progress = {"clipped": 0, "articles": 0, "errors": 0, "queue": 0,
                            "current_page": "", "account": "", "running": _is_clipper_running(),
                            "last_update": "", "page_time": "", "session_clips": 0, "clip_limit": 0}
                if log_files:
                    with open(log_files[0], "r", encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                    for line in lines:
                        if "Queue:" in line:
                            m = re.search(r'Queue:\s*(\d+)', line)
                            if m:
                                progress["queue"] = int(m.group(1))
                        if "Progress:" in line:
                            m = re.search(r'(\d+) clipped, (\d+) articles?, (\d+) errors?', line)
                            if m:
                                progress["clipped"] = int(m.group(1))
                                progress["articles"] = int(m.group(2))
                                progress["errors"] = int(m.group(3))
                            # Extract timestamp
                            tm = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                            if tm:
                                progress["last_update"] = tm.group(1)
                        if "Tracking as account:" in line:
                            m = re.search(r'Tracking as account:\s*(\S+)', line)
                            if m:
                                progress["account"] = m.group(1)
                        if "Rotated to" in line or "Switched to" in line:
                            m = re.search(r'(?:Rotated|Switched) to\s*(\S+@\S+\.com)', line)
                            if m:
                                progress["account"] = m.group(1)
                        if re.search(r'\[\d+/\d+\]', line):
                            m = re.search(r'\[\d+/\d+\]\s*(.*)', line)
                            if m:
                                progress["current_page"] = m.group(1).strip()
                        if "DONE" in line:
                            progress["running"] = False
                        # Extract per-page time
                        tm2 = re.search(r'Page:.*?(\d+\.\d+)s', line)
                        if tm2:
                            progress["page_time"] = tm2.group(1) + "s"
                # When not running, show next eligible account from DB (log account is stale)
                # When running, use the account from the log (most recent Tracking/Switched/Rotated)
                try:
                    acc_conn = sqlite3.connect(str(BASE_DIR / "lake_worth.db"))
                    acc_conn.row_factory = sqlite3.Row
                    acct_email = None
                    if not progress["running"]:
                        next_row = acc_conn.execute(
                            "SELECT email, clips_this_session FROM accounts WHERE active = 1"
                            " AND (last_throttle_time IS NULL OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))"
                            " ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC LIMIT 1"
                        ).fetchone()
                        if next_row:
                            progress["account"] = next_row["email"]
                            progress["session_clips"] = next_row["clips_this_session"] or 0
                            acct_email = next_row["email"]
                    else:
                        acct_email = progress["account"]
                        if acct_email:
                            acc_row = acc_conn.execute(
                                "SELECT clips_this_session FROM accounts WHERE email = ?",
                                (acct_email,)
                            ).fetchone()
                            if acc_row:
                                progress["session_clips"] = acc_row["clips_this_session"] or 0
                    # Count articles from pages clipped by this account (parsed from logs)
                    if acct_email and log_files:
                        acct_pages = []
                        is_acct = False
                        for lf in log_files:
                            try:
                                with open(lf, "r", encoding="utf-8", errors="replace") as lfile:
                                    for ln in lfile:
                                        if "Tracking as account:" in ln:
                                            is_acct = acct_email in ln
                                        elif "Rotated to" in ln or "Switched to" in ln:
                                            is_acct = acct_email in ln
                                        if is_acct and "Page:" in ln:
                                            pm = re.search(r'Page:\s*(\S+\.pdf)', ln)
                                            if pm:
                                                acct_pages.append(pm.group(1))
                            except Exception:
                                pass
                        if acct_pages:
                            placeholders = ",".join(["?"] * len(acct_pages))
                            art_count = acc_conn.execute(
                                f"SELECT COUNT(*) as c FROM articles WHERE pdf_filename IN ({placeholders})",
                                acct_pages
                            ).fetchone()
                            progress["articles"] = art_count["c"] if art_count else 0
                    acc_conn.close()
                except Exception:
                    pass
                # Look up clip limit and next eligible account from DB
                try:
                    state_conn = sqlite3.connect(str(BASE_DIR / "lake_worth.db"))
                    state_conn.row_factory = sqlite3.Row
                    lim_row = state_conn.execute(
                        "SELECT value FROM clipper_state WHERE key = 'daily_clip_limit'"
                    ).fetchone()
                    if lim_row:
                        progress["clip_limit"] = int(lim_row["value"] or 0)
                    # Find next eligible account (not throttled within 24h)
                    next_row = state_conn.execute(
                        "SELECT email, clips_this_session FROM accounts WHERE active = 1"
                        " AND (last_throttle_time IS NULL OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))"
                        " ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC LIMIT 1"
                    ).fetchone()
                    if next_row:
                        progress["next_account"] = next_row["email"]
                        progress["next_account_clips"] = next_row["clips_this_session"] or 0
                    state_conn.close()
                except Exception:
                    pass
                self.wfile.write(json.dumps(progress).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/log":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                log_files = sorted(glob.glob(str(LOG_DIR / "clipper_*.log")), key=os.path.getmtime, reverse=True)
                if log_files:
                    with open(log_files[0], "r", encoding="utf-8", errors="replace") as f:
                        all_lines = f.readlines()
                        tail = all_lines[-15:]
                        lines = "".join(tail)
                else:
                    lines = "No clipper log files found."
                self.wfile.write(json.dumps({"lines": lines}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/settings":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("CREATE TABLE IF NOT EXISTS clipper_state (key TEXT PRIMARY KEY, value TEXT)")
                conn.commit()
                settings = {}
                for row in conn.execute("SELECT key, value FROM clipper_state").fetchall():
                    settings[row["key"]] = row["value"]
                conn.close()
                # Add live status
                settings["stop_flag_active"] = STOP_FLAG.exists()
                settings["clipper_running"] = _is_clipper_running()
                self.wfile.write(json.dumps(settings, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/dismiss-entry":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            filename = body.get("filename", "")

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not filename:
                self.wfile.write(json.dumps({"error": "No filename"}).encode())
                return

            try:
                conn = get_db()
                conn.execute(
                    "UPDATE processed_pdfs SET articles_found = -1 WHERE pdf_filename = ?",
                    (filename,)
                )
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True, "dismissed": filename}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/toggle-highlight":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            table = body.get("table", "")
            identifier = body.get("identifier", "")
            highlighted = 1 if body.get("highlighted") else 0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not identifier or table not in ("articles", "processed_pdfs"):
                self.wfile.write(json.dumps({"error": "Invalid params"}).encode())
                return

            try:
                conn = get_db()
                if table == "articles":
                    conn.execute("UPDATE articles SET highlighted = ? WHERE id = ?", (highlighted, identifier))
                else:
                    conn.execute("UPDATE processed_pdfs SET highlighted = ? WHERE pdf_filename = ?", (highlighted, identifier))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/toggle-ignore":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            filename = body.get("filename", "")
            ignored = 1 if body.get("ignored") else 0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not filename:
                self.wfile.write(json.dumps({"error": "No filename"}).encode())
                return

            try:
                conn = get_db()
                conn.execute(
                    "UPDATE processed_pdfs SET ignored = ? WHERE pdf_filename = ?",
                    (ignored, filename)
                )
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True, "filename": filename, "ignored": ignored}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/toggle-photo":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            table = body.get("table", "")
            identifier = body.get("identifier", "")
            has_photo = 1 if body.get("has_photo") else 0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not identifier or table not in ("articles", "processed_pdfs"):
                self.wfile.write(json.dumps({"error": "Invalid params"}).encode())
                return

            try:
                conn = get_db()
                if table == "articles":
                    conn.execute("UPDATE articles SET has_photo = ? WHERE id = ?", (has_photo, identifier))
                else:
                    conn.execute("UPDATE processed_pdfs SET has_photo = ? WHERE pdf_filename = ?", (has_photo, identifier))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/delete-article":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            article_id = body.get("id")

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not article_id:
                self.wfile.write(json.dumps({"error": "No article ID"}).encode())
                return

            try:
                conn = get_db()
                # Get article info before deleting
                article = conn.execute("SELECT headline, pdf_filename FROM articles WHERE id = ?", (article_id,)).fetchone()
                if not article:
                    self.wfile.write(json.dumps({"error": "Article not found"}).encode())
                    return

                pdf_filename = article["pdf_filename"]

                conn.execute("DELETE FROM tags WHERE article_id = ?", (article_id,))
                conn.execute("DELETE FROM quotes WHERE article_id = ?", (article_id,))
                conn.execute("DELETE FROM images WHERE article_id = ?", (article_id,))
                conn.execute("DELETE FROM articles WHERE id = ?", (article_id,))

                # Update processed_pdfs count
                if pdf_filename:
                    count = conn.execute(
                        "SELECT COUNT(*) as c FROM articles WHERE pdf_filename = ?",
                        (pdf_filename,)
                    ).fetchone()["c"]
                    conn.execute(
                        "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
                        (count, pdf_filename),
                    )

                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True, "deleted": article_id}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/import-clip-data":
            # Accept pre-extracted clip data (from bookmarklet)
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            try:
                clip_data = {
                    "date": body.get("date", ""),
                    "page": body.get("page", 0),
                    "newspaper": body.get("newspaper", ""),
                    "headline": body.get("headline", ""),
                    "ocr_text": body.get("text", ""),
                    "source_url": body.get("url", ""),
                    "clip_id": body.get("clip_id", ""),
                    "image_url": body.get("image_url", ""),
                }

                if not clip_data.get("ocr_text") and not clip_data.get("headline"):
                    self.wfile.write(json.dumps({"error": "No text found in clip data"}).encode())
                    return

                result = import_clip(clip_data)
                if result.get("error"):
                    self.wfile.write(json.dumps(result).encode())
                    return
                result["clip"] = clip_data
                self.wfile.write(json.dumps(result, default=str).encode())
            except Exception as e:
                import traceback
                traceback.print_exc()
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/import-clip":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            url = body.get("url", "").strip()
            replace_article_id = body.get("replace_article_id")

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not url or "newspapers.com" not in url:
                self.wfile.write(json.dumps({"error": "Invalid clip URL"}).encode())
                return

            try:
                clip_data = fetch_clip(url)
                if not clip_data.get("ocr_text") and not clip_data.get("headline"):
                    self.wfile.write(json.dumps({"error": "Could not extract text from clip", "raw": clip_data}).encode())
                    return

                result = import_clip(clip_data, replace_article_id)
                result["clip"] = clip_data
                self.wfile.write(json.dumps(result, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/save":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                acct_id = body.get("id")
                if acct_id:
                    conn.execute("""
                        UPDATE accounts SET email=?, password=?, label=?, subscription_type=?,
                        active=?, notes=?, updated_at=datetime('now','localtime')
                        WHERE id=?
                    """, (body["email"], body["password"], body.get("label",""),
                          body.get("subscription_type",""), body.get("active",1),
                          body.get("notes",""), acct_id))
                else:
                    conn.execute("""
                        INSERT INTO accounts (email, password, label, subscription_type, active, notes)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (body["email"], body["password"], body.get("label",""),
                          body.get("subscription_type",""), body.get("active",1),
                          body.get("notes","")))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/delete":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("DELETE FROM accounts WHERE id=?", (body["id"],))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/reset-session":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("""
                    UPDATE accounts SET clips_this_session=0, updated_at=datetime('now','localtime')
                    WHERE id=?
                """, (body["id"],))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/settings":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("CREATE TABLE IF NOT EXISTS clipper_state (key TEXT PRIMARY KEY, value TEXT)")
                for key, value in body.items():
                    conn.execute(
                        "INSERT INTO clipper_state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
                        (key, str(value), str(value))
                    )
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/start":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Check if already running
                if _is_clipper_running():
                    self.wfile.write(json.dumps({"error": "Clipper is already running."}).encode())
                    return
                # Clear stop flag if present
                if STOP_FLAG.exists():
                    if STOP_FLAG.is_dir():
                        STOP_FLAG.rmdir()
                    else:
                        STOP_FLAG.unlink()
                # Build command
                account = body.get("account", "").strip()
                date_start = body.get("date_start", "").strip()
                date_end = body.get("date_end", "").strip()
                cmd = ["python", "clip_and_extract.py", "0"]
                if date_start:
                    cmd.append(date_start)
                    cmd.append(date_end or "2025-12-31")
                    if account:
                        cmd.append(account)
                elif account:
                    cmd.extend(["", "", account])
                # Launch as independent process (CREATE_NEW_PROCESS_GROUP so it survives
                # parent exit, but NOT DETACHED_PROCESS which hides the window and
                # prevents Chrome from getting focus for mouse interactions)
                subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
                msg = f"Clipper started"
                if account:
                    msg += f" with {account}"
                else:
                    msg += " with auto-rotate"
                if date_start:
                    msg += f" ({date_start} to {date_end or '2025-12-31'})"
                self.wfile.write(json.dumps({"ok": True, "message": msg}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/stop":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                STOP_FLAG.mkdir(exist_ok=True)
                self.wfile.write(json.dumps({"ok": True, "message": "Stop flag set. Clipper will stop after current page."}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/kill":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Find and kill clip_and_extract.py processes
                result = subprocess.run(
                    ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
                    capture_output=True, text=True, timeout=5
                )
                killed = []
                for line in result.stdout.splitlines():
                    if "clip_and_extract" in line:
                        parts = line.strip().split()
                        for part in parts:
                            if part.isdigit():
                                pid = int(part)
                                try:
                                    os.kill(pid, signal.SIGTERM)
                                    killed.append(pid)
                                except Exception:
                                    pass
                self.wfile.write(json.dumps({"ok": True, "killed_pids": killed}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()

    def log_message(self, format, *args):
        pass  # Suppress request logging


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    daemon_threads = True


def main():
    server = ThreadedHTTPServer(("localhost", PORT), DashboardHandler)
    print(f"Dashboard running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == "__main__":
    main()
