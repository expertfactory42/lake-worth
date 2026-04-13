"""One-off script: re-run AI article extraction on clipped pages that have
articles_found=0 but contain 'lake worth' in their OCR text.

Does NOT re-clip — uses the existing ocr_text already in the DB.
Useful after bumping max_tokens or fixing extraction bugs.

Usage:
    python reextract_no_articles.py          # process all qualifying pages
    python reextract_no_articles.py --dry    # preview without changes
"""

import sys
import time
import sqlite3
import re
import json
import logging
from pathlib import Path

DB_PATH = r"c:\lake_worth\lake_worth.db"
SEARCH_TERM = "lake worth"
RUNTIME_DIR = Path(r"C:\lake_worth_runtime")
STATUS_FILE = RUNTIME_DIR / "reextract_status.json"
STOP_FLAG = RUNTIME_DIR / "stop_reextract"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("reextract")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def parse_pdf_filename(pdf_filename):
    """Extract newspaper, date, page from pdf_filename like
    Fort_Worth_Star_Telegram_1919_05_13_22.pdf"""
    m = re.match(r"(.+?)_(\d{4})_(\d{2})_(\d{2})_(\d+)\.pdf", pdf_filename)
    if not m:
        return None, None, None
    newspaper = m.group(1).replace("_", " ")
    date_str = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
    page = m.group(5)
    return newspaper, date_str, page


def extract_articles_with_ai(ocr_text, date_str, newspaper, page):
    """Same extraction logic as clip_and_extract.py with 16384 max_tokens."""
    if not ocr_text or len(ocr_text) < 20:
        return []

    if not re.search(r'(?i)lake[\s.\-,;:]+worth', ocr_text):
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

        if "```" in result_text:
            m = re.search(r'```(?:json)?\s*(.*?)```', result_text, re.DOTALL)
            if m:
                result_text = m.group(1).strip()

        articles = json.loads(result_text)
        if isinstance(articles, list):
            return articles
        return []

    except json.JSONDecodeError as e:
        log.warning(f"    AI returned invalid JSON: {e}")
        return None
    except Exception as e:
        log.warning(f"    AI extraction error: {e}")
        return None


def save_articles(conn, pdf_filename, articles, search_term, clip_url=""):
    """Save extracted articles to the articles table."""
    count = 0
    for art in articles:
        headline = art.get("headline", "Untitled")
        text = art.get("text", "")
        photo_desc = art.get("photo_description", "")
        has_photo = 1 if photo_desc else 0

        newspaper, date_str, page = parse_pdf_filename(pdf_filename)

        # Check for duplicate
        existing = conn.execute(
            "SELECT id FROM articles WHERE pdf_filename = ? AND headline = ?",
            (pdf_filename, headline),
        ).fetchone()
        if existing:
            continue

        conn.execute(
            """INSERT INTO articles
               (date, newspaper, page, headline, full_text, pdf_filename,
                search_term, clip_id, has_photo, photo_description, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now','localtime'))""",
            (date_str, newspaper, page, headline, text, pdf_filename,
             search_term, clip_url, has_photo, photo_desc),
        )
        count += 1
    return count


def _write_status(data):
    """Write progress to status file for dashboard polling."""
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    try:
        STATUS_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


def main():
    dry_run = "--dry" in sys.argv

    # Clean up any leftover stop flag
    if STOP_FLAG.exists():
        STOP_FLAG.unlink(missing_ok=True)

    conn = get_db()

    rows = conn.execute("""
        SELECT pdf_filename, ocr_text, clip_url
        FROM processed_pdfs
        WHERE clipped = 1
          AND articles_found = 0
          AND ocr_text IS NOT NULL
          AND length(ocr_text) > 100
          AND lower(ocr_text) LIKE '%lake%worth%'
        ORDER BY date_str
    """).fetchall()

    log.info(f"Found {len(rows)} pages to re-extract")
    _write_status({"status": "running", "processed": 0, "total": len(rows),
                   "articles": 0, "failed": 0, "current": ""})

    if dry_run:
        for r in rows:
            log.info(f"  [DRY] {r['pdf_filename']}")
        _write_status({"status": "done_dry", "processed": 0, "total": len(rows),
                       "articles": 0, "failed": 0, "current": ""})
        return

    total_articles = 0
    success = 0
    failed = 0

    for i, row in enumerate(rows, 1):
        # Check stop flag
        if STOP_FLAG.exists():
            log.info("Stop flag detected — halting")
            STOP_FLAG.unlink(missing_ok=True)
            _write_status({"status": "stopped", "processed": success, "total": len(rows),
                           "articles": total_articles, "failed": failed, "current": ""})
            conn.close()
            return

        pdf = row["pdf_filename"]
        ocr = row["ocr_text"]
        clip_url = row["clip_url"] or ""

        newspaper, date_str, page = parse_pdf_filename(pdf)
        if not newspaper:
            log.warning(f"  [{i}/{len(rows)}] Could not parse: {pdf}")
            failed += 1
            _write_status({"status": "running", "processed": success, "total": len(rows),
                           "articles": total_articles, "failed": failed, "current": pdf})
            continue

        log.info(f"  [{i}/{len(rows)}] {pdf} ({len(ocr)} chars)")
        _write_status({"status": "running", "processed": success, "total": len(rows),
                       "articles": total_articles, "failed": failed, "current": pdf})

        articles = extract_articles_with_ai(ocr, date_str, newspaper, page)

        if articles is None:
            log.warning(f"    Extraction failed — skipping")
            failed += 1
            continue

        if articles:
            count = save_articles(conn, pdf, articles, SEARCH_TERM, clip_url=clip_url)
            conn.execute(
                "UPDATE processed_pdfs SET articles_found = ? WHERE pdf_filename = ?",
                (count, pdf),
            )
            conn.commit()
            total_articles += count
            log.info(f"    Found {count} articles (total so far: {total_articles})")
        else:
            conn.execute(
                "UPDATE processed_pdfs SET articles_found = 0 WHERE pdf_filename = ?",
                (pdf,),
            )
            conn.commit()
            log.info(f"    No Lake Worth articles found")

        success += 1
        time.sleep(0.5)  # gentle rate limit on API

    _write_status({"status": "done", "processed": success, "total": len(rows),
                   "articles": total_articles, "failed": failed, "current": ""})
    log.info(f"\nDone: {success} processed, {failed} failed, {total_articles} articles extracted")
    conn.close()


if __name__ == "__main__":
    main()
