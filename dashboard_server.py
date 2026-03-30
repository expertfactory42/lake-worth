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
import urllib.request
import urllib.error
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

BASE_DIR = Path(r"C:\lake_worth")
DB_PATH = BASE_DIR / "lake_worth.db"
PDF_DIR = BASE_DIR / "pdfs"
PORT = 8765


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


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

    # Processed PDFs with no articles found
    no_article_rows = conn.execute("""
        SELECT pp.pdf_filename, pp.processed_at, pp.search_term, pp.url, pp.clip_url
        FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        ORDER BY pp.pdf_filename
    """).fetchall()

    no_articles_list = []
    for row in no_article_rows:
        fname = row["pdf_filename"]
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

        no_articles_list.append({
            "filename": fname,
            "date": date_str,
            "page": page,
            "newspaper": newspaper,
            "search_term": row["search_term"],
            "url": row["url"] if "url" in row.keys() else "",
            "clip_url": row["clip_url"] if "clip_url" in row.keys() else "",
        })

    no_articles_list.sort(key=lambda x: x["date"], reverse=True)

    # Monthly reference counts (articles + no-articles combined)
    monthly = {}
    for a in article_list:
        if a.get("date") and len(a["date"]) >= 7:
            m = a["date"][:7]
            monthly[m] = monthly.get(m, 0) + 1
    for na in no_articles_list:
        if na.get("date") and len(na["date"]) >= 7 and na["date"] != "?":
            m = na["date"][:7]
            monthly[m] = monthly.get(m, 0) + 1
    monthly_sorted = sorted(monthly.items())

    # Articles by year
    articles_by_year = {}
    for a in article_list:
        if a.get("date") and len(a["date"]) >= 4:
            y = a["date"][:4]
            articles_by_year[y] = articles_by_year.get(y, 0) + 1
    articles_by_year_sorted = sorted(articles_by_year.items())

    conn.close()

    return {
        "stats": stats,
        "articles": article_list,
        "quotes": quote_list,
        "people": people_list,
        "log": log_list,
        "no_articles": no_articles_list,
        "monthly": monthly_sorted,
        "articles_by_year": articles_by_year_sorted,
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
    """Fetch a URL using headless Chrome with the saved login profile."""
    import time
    import undetected_chromedriver as uc

    # Use a copy of the login profile to avoid locking the main one
    profile_src = BASE_DIR / "chrome_temp_profile"
    profile_dst = BASE_DIR / "chrome_temp_profile_server"

    # Copy profile if it doesn't exist yet (or is stale)
    if not (profile_dst / "Default").exists() and (profile_src / "Default").exists():
        import shutil
        if profile_dst.exists():
            shutil.rmtree(profile_dst, ignore_errors=True)
        shutil.copytree(str(profile_src), str(profile_dst), dirs_exist_ok=True)

    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={str(profile_dst)}")
    options.add_argument("--headless=new")
    options.add_argument("--no-first-run")
    options.add_argument("--disable-gpu")
    driver = None
    try:
        driver = uc.Chrome(options=options, version_main=145)
        driver.get(url)
        time.sleep(6)
        return driver.page_source
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


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
        conn.execute(
            """UPDATE articles SET full_text = ?, headline = ?, has_image = ?,
                                   clip_id = ?, newspaper = ?
               WHERE id = ?""",
            (ocr_text, use_headline, has_image, clip_id or None, newspaper, match_id),
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
        cur = conn.execute(
            """INSERT INTO articles (date, newspaper, page, headline, full_text,
                                     pdf_filename, has_image, search_term, clip_id)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (date, newspaper, page, headline, ocr_text, pdf_filename, has_image, search_term, clip_id or None),
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
