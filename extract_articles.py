"""
extract_articles.py

DocuPipe standardization + Claude pipeline for extracting Lake Worth articles.

Step 1: DocuPipe parse + standardize — extracts all articles from the page
Step 2: Filter articles containing "Lake Worth"
Step 3: Claude — enriches with quotes, people, tags
Step 4: Claude Vision — finds photos/illustrations

Usage:
    set ANTHROPIC_API_KEY=sk-...
    set DOCUPIPE_API_KEY=...
    python extract_articles.py
"""

import os
import re
import sys
import json
import time
import base64
import sqlite3
import logging
import traceback
from pathlib import Path
from io import BytesIO

import fitz  # PyMuPDF
import requests
from PIL import Image
import anthropic

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DIR = Path(r"C:\lake_worth")
PDF_DIR = BASE_DIR / "pdfs"
IMAGE_DIR = BASE_DIR / "images"
DB_PATH = BASE_DIR / "lake_worth.db"
CACHE_DIR = BASE_DIR / "docupipe_cache"

CLAUDE_MODEL = "claude-sonnet-4-20250514"
MAX_TOKENS = 8192
DPI = 300

DOCUPIPE_BASE = "https://app.docupipe.ai"
DOCUPIPE_SCHEMA = "BQNNnBPR"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    date            TEXT,
    newspaper       TEXT,
    page            INTEGER,
    headline        TEXT,
    full_text       TEXT,
    pdf_filename    TEXT,
    has_image       BOOLEAN DEFAULT 0,
    created_at      TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS quotes (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    quote_text      TEXT,
    speaker         TEXT,
    speaker_role    TEXT,
    context         TEXT,
    article_id      INTEGER REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS people (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    name            TEXT UNIQUE,
    role            TEXT,
    first_seen_date TEXT
);

CREATE TABLE IF NOT EXISTS images (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    cropped_image_file  TEXT,
    caption             TEXT,
    description         TEXT,
    article_id          INTEGER REFERENCES articles(id),
    pdf_filename        TEXT
);

CREATE TABLE IF NOT EXISTS tags (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    article_id  INTEGER REFERENCES articles(id),
    tag         TEXT
);

CREATE TABLE IF NOT EXISTS processed_pdfs (
    pdf_filename    TEXT PRIMARY KEY,
    processed_at    TEXT DEFAULT (datetime('now')),
    articles_found  INTEGER DEFAULT 0
);
"""


def init_db() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# Filename parsing
# ---------------------------------------------------------------------------

FILENAME_RE = re.compile(
    r"^(?P<paper>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<page>\d+)\.pdf$",
    re.IGNORECASE,
)


def parse_filename(filename: str):
    m = FILENAME_RE.match(filename)
    if not m:
        return None
    newspaper = m.group("paper").replace("_", " ")
    date_str = f"{m.group('year')}-{m.group('month')}-{m.group('day')}"
    page = int(m.group("page"))
    return newspaper, date_str, page


# ---------------------------------------------------------------------------
# DocuPipe: upload → parse → standardize
# ---------------------------------------------------------------------------


def docupipe_upload_and_parse(pdf_path: Path, api_key: str) -> str:
    """Upload PDF, wait for parse, return document ID."""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    with open(pdf_path, "rb") as f:
        b64 = base64.b64encode(f.read()).decode()

    resp = requests.post(
        f"{DOCUPIPE_BASE}/document",
        headers=headers,
        json={"document": {"file": {"contents": b64, "filename": pdf_path.name}}},
        timeout=60,
    )
    resp.raise_for_status()
    data = resp.json()
    doc_id = data["documentId"]
    job_id = data["jobId"]

    # Wait for parse completion
    for _ in range(24):
        time.sleep(5)
        job_resp = requests.get(f"{DOCUPIPE_BASE}/job/{job_id}",
                                headers={"X-API-Key": api_key}, timeout=30)
        if job_resp.json().get("status") == "completed":
            break
    else:
        raise TimeoutError(f"DocuPipe parse job {job_id} timed out")

    return doc_id


def docupipe_standardize(doc_id: str, api_key: str, display_mode: str = None) -> dict:
    """Run standardization on a parsed document, return structured data."""
    headers = {"X-API-Key": api_key, "Content-Type": "application/json"}

    payload = {"schemaId": DOCUPIPE_SCHEMA, "documentIds": [doc_id]}
    if display_mode:
        payload["displayMode"] = display_mode

    resp = requests.post(
        f"{DOCUPIPE_BASE}/v2/standardize/batch",
        headers=headers,
        json=payload,
        timeout=60,
    )
    resp.raise_for_status()
    batch = resp.json()
    std_id = batch.get("standardizationIds", [""])[0]
    batch_job = batch.get("jobId", "")

    # Wait for standardization
    for _ in range(30):
        time.sleep(5)
        job_resp = requests.get(f"{DOCUPIPE_BASE}/job/{batch_job}",
                                headers={"X-API-Key": api_key}, timeout=30)
        if job_resp.json().get("status") == "completed":
            break
    else:
        raise TimeoutError(f"DocuPipe standardize job {batch_job} timed out")

    # Get result
    sr = requests.get(f"{DOCUPIPE_BASE}/standardization/{std_id}",
                      headers={"X-API-Key": api_key}, timeout=30)
    sr.raise_for_status()
    return sr.json().get("data", {})


def _is_garbled(text: str) -> bool:
    """Detect if article text has significant OCR garbling."""
    words = re.findall(r'[a-zA-Z]+', text)
    if len(words) < 10:
        return False
    # Words with no vowels (>3 chars)
    no_vowel = sum(1 for w in words if len(w) > 3 and not re.search(r'[aeiouAEIOU]', w))
    # Words with 5+ consecutive consonants
    garbled = sum(1 for w in words if re.search(r'[bcdfghjklmnpqrstvwxz]{5,}', w, re.IGNORECASE))
    bad_ratio = (no_vowel + garbled) / len(words)
    return bad_ratio > 0.05


def find_matching_articles(std_data: dict, search_term: str) -> list[dict]:
    """Filter standardized articles that mention the search term."""
    term = search_term.lower()
    matches = []
    for article in std_data.get("headlines", []):
        title = article.get("title", "") or ""
        subtitle = article.get("subtitle", "") or ""
        full_text = article.get("fullText", "") or ""
        topic = article.get("topic", "") or ""
        combined = f"{title} {subtitle} {full_text} {topic}".lower()
        if term in combined:
            matches.append(article)
    return matches


# ---------------------------------------------------------------------------
# Claude: enrich articles with quotes, people, tags
# ---------------------------------------------------------------------------

ENRICH_PROMPT = """\
This is an article from {newspaper}, {date}, page {page}.

HEADLINE: {headline}
SUBTITLE: {subtitle}
TOPIC: {topic}

FULL TEXT:
{full_text}

Extract the following from this article and return as JSON:

{{
  "quotes": [{{"quote_text": "exact quoted text in quotation marks", "speaker": "who said it", "speaker_role": "their title/role", "context": "brief context"}}],
  "people": [{{"name": "full name as printed", "role": "title/role if mentioned"}}],
  "tags": ["topic1", "topic2"]
}}

RULES:
- Only include actual direct quotes (text in quotation marks with speaker attribution).
- Include ALL named people with their roles/titles.
- Tags: "city council", "real estate", "dam", "fishing", "crime", "social", "sports", "school", "church", "infrastructure", "legal", "advertisement", "development", "water", "politics", etc.
"""


def claude_enrich(client: anthropic.Anthropic, article: dict,
                  newspaper: str, date_str: str, page: int) -> dict:
    """Send article text to Claude for quote/people/tag extraction."""
    prompt = ENRICH_PROMPT.format(
        newspaper=newspaper,
        date=date_str,
        page=page,
        headline=article.get("title", "") or "",
        subtitle=article.get("subtitle", "") or "",
        topic=article.get("topic", "") or "",
        full_text=article.get("fullText", "") or "",
    )
    response = client.messages.create(
        model=CLAUDE_MODEL,
        max_tokens=4096,
        messages=[{"role": "user", "content": prompt}],
    )
    text = response.content[0].text

    json_match = re.search(r"```json\s*(.*?)\s*```", text, re.DOTALL)
    if json_match:
        text = json_match.group(1)
    else:
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            text = json_match.group(0)

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        log.warning("  Enrich JSON parse failed")
        return {"quotes": [], "people": [], "tags": []}


# ---------------------------------------------------------------------------
# Claude vision: detect photos/illustrations
# ---------------------------------------------------------------------------


def pdf_page_to_image(pdf_path: Path, page_num: int = 0) -> Image.Image:
    doc = fitz.open(str(pdf_path))
    page = doc.load_page(page_num)
    pix = page.get_pixmap(matrix=fitz.Matrix(DPI / 72, DPI / 72), alpha=False)
    img = Image.frombytes("RGB", [pix.width, pix.height], pix.samples)
    doc.close()
    return img


def image_to_base64(img: Image.Image, max_long_edge: int = 2000) -> str:
    w, h = img.size
    if max(w, h) > max_long_edge:
        scale = max_long_edge / max(w, h)
        img = img.resize((int(w * scale), int(h * scale)), Image.LANCZOS)
    buf = BytesIO()
    img.save(buf, format="JPEG", quality=85)
    return base64.standard_b64encode(buf.getvalue()).decode("ascii")


def detect_images_on_page(client: anthropic.Anthropic, page_img: Image.Image) -> list[dict]:
    img_b64 = image_to_base64(page_img)
    prompt = """This is a scanned newspaper page. Are there any PHOTOGRAPHS, ILLUSTRATIONS, or MAPS related to "Lake Worth"?

If yes, return JSON: {"images": [{"caption": "caption text", "description": "what it shows", "bbox_pct": [left%, top%, right%, bottom%]}]}
If no, return: {"images": []}

Only include actual photographs/illustrations, not text."""

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=1000,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
                {"type": "text", "text": prompt},
            ]}],
        )
        text = response.content[0].text
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0)).get("images", [])
    except Exception as e:
        log.warning("  Image detection error: %s", e)
    return []


# ---------------------------------------------------------------------------
# Claude vision: find ALL mentions of search term on page
# ---------------------------------------------------------------------------

MENTION_FINDER_PROMPT = """\
This is a scanned newspaper page from {newspaper}, {date}, page {page}.

Find EVERY mention of "{search_term}" on this page. Include:
- Full articles with headlines
- Short blurbs, notices, and briefs (even 1-2 sentences)
- Items WITHOUT headlines (classified ads, social notes, meeting notices, etc.)
- Partial articles that continue from or to another page

For each mention, return:
- "headline": the headline IF one exists, otherwise null
- "text_on_page": the visible text on THIS page related to the mention (transcribe it)
- "item_type": one of "article", "brief", "notice", "ad", "social", "legal", "sports_score", "continuation", "other"

Return JSON:
{{"mentions": [{{"headline": "...", "text_on_page": "...", "item_type": "..."}}]}}

If no mentions of "{search_term}" are found, return: {{"mentions": []}}

IMPORTANT: Transcribe text carefully from the scan. Include ALL mentions, no matter how small."""


def find_mentions_via_vision(client: anthropic.Anthropic, page_img: Image.Image,
                              newspaper: str, date_str: str, page: int,
                              search_term: str) -> list[dict]:
    """Use Claude vision to find all mentions of search term on the page."""
    img_b64 = image_to_base64(page_img)
    prompt = MENTION_FINDER_PROMPT.format(
        newspaper=newspaper, date=date_str, page=page, search_term=search_term
    )

    try:
        response = client.messages.create(
            model=CLAUDE_MODEL, max_tokens=MAX_TOKENS,
            messages=[{"role": "user", "content": [
                {"type": "image", "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}},
                {"type": "text", "text": prompt},
            ]}],
        )
        text = response.content[0].text
        json_match = re.search(r"\{.*\}", text, re.DOTALL)
        if json_match:
            return json.loads(json_match.group(0)).get("mentions", [])
    except Exception as e:
        log.warning("  Vision mention finder error: %s", e)
    return []


def merge_docupipe_and_vision(docupipe_articles: list[dict], vision_mentions: list[dict],
                                search_term: str) -> list[dict]:
    """Merge DocuPipe articles with vision-found mentions, deduplicating."""
    # Start with DocuPipe articles (they have structured fullText)
    merged = []
    seen_headlines = set()

    for art in docupipe_articles:
        headline = (art.get("title", "") or "").strip().lower()
        if headline:
            seen_headlines.add(headline)
        merged.append({
            "headline": art.get("title", "") or "",
            "subtitle": art.get("subtitle", "") or "",
            "full_text": art.get("fullText", "") or "",
            "item_type": "article",
            "source": "docupipe",
        })

    # Add vision mentions that DocuPipe missed
    for mention in vision_mentions:
        headline = (mention.get("headline") or "").strip().lower()
        text = (mention.get("text_on_page") or "").strip()
        if not text:
            continue

        # Skip if DocuPipe already found this (fuzzy match on headline)
        if headline and any(headline in h or h in headline for h in seen_headlines if h):
            continue

        # Skip if the text is substantially contained in an existing article
        text_lower = text.lower()[:100]
        already_found = False
        for existing in merged:
            existing_text = (existing.get("full_text") or "").lower()
            if text_lower and len(text_lower) > 20 and text_lower in existing_text:
                already_found = True
                break
        if already_found:
            continue

        merged.append({
            "headline": mention.get("headline") or "",
            "subtitle": "",
            "full_text": text,
            "item_type": mention.get("item_type", "other"),
            "source": "vision",
        })

    return merged


# ---------------------------------------------------------------------------
# Image cropping
# ---------------------------------------------------------------------------


def crop_and_save_image(page_img: Image.Image, bbox_pct: list,
                        pdf_filename: str, image_index: int) -> str:
    w, h = page_img.size
    left = max(0, min(int(bbox_pct[0] / 100.0 * w), w))
    top = max(0, min(int(bbox_pct[1] / 100.0 * h), h))
    right = max(left + 1, min(int(bbox_pct[2] / 100.0 * w), w))
    bottom = max(top + 1, min(int(bbox_pct[3] / 100.0 * h), h))

    cropped = page_img.crop((left, top, right, bottom))
    stem = Path(pdf_filename).stem
    out_name = f"{stem}_img{image_index}.jpg"
    (IMAGE_DIR / out_name).parent.mkdir(parents=True, exist_ok=True)
    cropped.save(str(IMAGE_DIR / out_name), format="JPEG", quality=90)
    return out_name


# ---------------------------------------------------------------------------
# Store results
# ---------------------------------------------------------------------------


def store_results(conn, articles, newspaper, date_str, page, pdf_filename, page_img,
                  search_term="lake worth"):
    img_counter = 0
    for art in articles:
        has_image = bool(art.get("images"))
        cur = conn.execute(
            """INSERT INTO articles (date, newspaper, page, headline, full_text,
                                     pdf_filename, has_image, search_term)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (date_str, newspaper, page, art.get("headline", ""),
             art.get("full_text", ""), pdf_filename, has_image, search_term),
        )
        article_id = cur.lastrowid

        for q in art.get("quotes", []):
            conn.execute(
                """INSERT INTO quotes (quote_text, speaker, speaker_role, context, article_id)
                   VALUES (?, ?, ?, ?, ?)""",
                (q.get("quote_text", ""), q.get("speaker", ""),
                 q.get("speaker_role", ""), q.get("context", ""), article_id),
            )

        for p in art.get("people", []):
            name = (p.get("name", "") or "").strip()
            if not name:
                continue
            conn.execute(
                """INSERT INTO people (name, role, first_seen_date)
                   VALUES (?, ?, ?)
                   ON CONFLICT(name) DO UPDATE SET
                       role = CASE WHEN excluded.first_seen_date < people.first_seen_date
                                   THEN excluded.role ELSE people.role END,
                       first_seen_date = MIN(people.first_seen_date, excluded.first_seen_date)
                """,
                (name, p.get("role", "") or "", date_str),
            )

        for tag in art.get("tags", []):
            conn.execute("INSERT INTO tags (article_id, tag) VALUES (?, ?)",
                         (article_id, tag))

        for img_info in art.get("images", []):
            bbox = img_info.get("bbox_pct")
            cropped_file = ""
            if bbox and len(bbox) == 4 and page_img:
                try:
                    img_counter += 1
                    cropped_file = crop_and_save_image(page_img, bbox, pdf_filename, img_counter)
                except Exception as e:
                    log.warning("Failed to crop image: %s", e)
            conn.execute(
                """INSERT INTO images (cropped_image_file, caption, description,
                                       article_id, pdf_filename)
                   VALUES (?, ?, ?, ?, ?)""",
                (cropped_file, img_info.get("caption", ""),
                 img_info.get("description", ""), article_id, pdf_filename),
            )

    conn.commit()
    return len(articles)


# ---------------------------------------------------------------------------
# Process one PDF
# ---------------------------------------------------------------------------


def process_pdf(pdf_path, docupipe_key, claude_client, newspaper, date_str, page,
                search_term="lake worth"):
    """DocuPipe standardize → filter by search term → vision mention finder → Claude enrich."""

    # Step 1: Upload and parse
    log.info("  DocuPipe upload + parse...")
    doc_id = docupipe_upload_and_parse(pdf_path, docupipe_key)

    # Step 2: Standardize
    log.info("  DocuPipe standardize...")
    std_data = docupipe_standardize(doc_id, docupipe_key)

    # Cache raw standardization result
    CACHE_DIR.mkdir(exist_ok=True)
    cache_name = Path(pdf_path).stem + ".json"
    with open(CACHE_DIR / cache_name, "w", encoding="utf-8") as f:
        json.dump(std_data, f, indent=2, ensure_ascii=False)

    total_articles = len(std_data.get("headlines", []))
    log.info("  Found %d total articles on page", total_articles)

    # Step 3: Filter by search term
    dp_articles = find_matching_articles(std_data, search_term)
    log.info("  %d article(s) from DocuPipe mention '%s'", len(dp_articles), search_term)

    # Step 3b: Retry garbled articles with image mode
    for i, art in enumerate(dp_articles):
        text = art.get("fullText", "") or ""
        if text and _is_garbled(text):
            log.info("  Garbled text detected, retrying with image mode...")
            img_data = docupipe_standardize(doc_id, docupipe_key, display_mode="image")
            img_articles = find_matching_articles(img_data, search_term)
            headline = (art.get("title", "") or "").lower()
            for img_art in img_articles:
                img_text = img_art.get("fullText", "") or ""
                img_title = (img_art.get("title", "") or "").lower()
                if img_text and img_title and headline and img_title in headline or headline in img_title:
                    log.info("  Image mode produced better text (%d chars)", len(img_text))
                    dp_articles[i] = img_art
                    break
            break  # only retry once per page

    # Step 4: Vision pass — find ALL mentions including blurbs DocuPipe missed
    page_img = pdf_page_to_image(pdf_path)
    log.info("  Vision mention finder for '%s'...", search_term)
    vision_mentions = find_mentions_via_vision(
        claude_client, page_img, newspaper, date_str, page, search_term
    )
    log.info("  Vision found %d mention(s)", len(vision_mentions))

    # Step 5: Merge DocuPipe + vision results
    merged = merge_docupipe_and_vision(dp_articles, vision_mentions, search_term)
    log.info("  %d total items after merge (%d from DocuPipe, %d new from vision)",
             len(merged), len(dp_articles), len(merged) - len(dp_articles))

    if not merged:
        return {"articles": [], "page_img": page_img}

    # Step 6: Enrich each item with Claude
    enriched = []
    for item in merged:
        headline = item.get("headline", "") or ""
        subtitle = item.get("subtitle", "") or ""
        full_text = item.get("full_text", "") or ""

        display_name = headline[:60] if headline else f"[{item.get('item_type', 'item')}] {full_text[:50]}"
        log.info("  Enriching: %s", display_name)

        # Build a pseudo-article dict for the enrich prompt
        art_for_enrich = {
            "title": headline,
            "subtitle": subtitle,
            "topic": item.get("item_type", ""),
            "fullText": full_text,
        }
        extra = claude_enrich(claude_client, art_for_enrich, newspaper, date_str, page)

        enriched.append({
            "headline": f"{headline} — {subtitle}".strip(" —") if subtitle else headline,
            "full_text": full_text,
            "quotes": extra.get("quotes", []),
            "people": extra.get("people", []),
            "tags": extra.get("tags", []),
            "images": [],
        })

    # Step 7: Check for photos/illustrations
    images = detect_images_on_page(claude_client, page_img)
    if images and enriched:
        enriched[0]["images"] = images
        log.info("  Found %d image(s) on page", len(images))

    return {"articles": enriched, "page_img": page_img}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def get_sorted_pdfs():
    pdfs = sorted(PDF_DIR.glob("*.pdf"))
    decorated = []
    for p in pdfs:
        parsed = parse_filename(p.name)
        if parsed:
            decorated.append((parsed[1], parsed[2], p))
        else:
            decorated.append(("9999-99-99", 0, p))
    decorated.sort()
    return [item[2] for item in decorated]


def already_processed(conn):
    return {r[0] for r in conn.execute("SELECT pdf_filename FROM processed_pdfs").fetchall()}


def get_search_term(conn, filename: str) -> str:
    """Look up the search term for a PDF. Defaults to 'lake worth'."""
    row = conn.execute(
        "SELECT search_term FROM processed_pdfs WHERE pdf_filename = ?", (filename,)
    ).fetchone()
    if row and row[0]:
        return row[0]
    # Infer from filename date
    parsed = parse_filename(filename)
    if parsed:
        _, date_str, _ = parsed
        if date_str[:4] == "1909":
            return "fire"
        if "1913-07-04" <= date_str <= "1913-12-06":
            return "minnetonka"
    return "lake worth"


def mark_processed(conn, filename, articles_found=0, search_term="lake worth"):
    conn.execute(
        "INSERT OR REPLACE INTO processed_pdfs (pdf_filename, articles_found, search_term) VALUES (?, ?, ?)",
        (filename, articles_found, search_term),
    )
    conn.commit()


def main():
    anthropic_key = os.environ.get("ANTHROPIC_API_KEY")
    docupipe_key = os.environ.get("DOCUPIPE_API_KEY")
    if not anthropic_key:
        log.error("ANTHROPIC_API_KEY not set."); sys.exit(1)
    if not docupipe_key:
        log.error("DOCUPIPE_API_KEY not set."); sys.exit(1)

    claude_client = anthropic.Anthropic(api_key=anthropic_key)
    IMAGE_DIR.mkdir(parents=True, exist_ok=True)

    conn = init_db()
    done = already_processed(conn)
    pdfs = get_sorted_pdfs()
    remaining = [p for p in pdfs if p.name not in done]

    log.info("Found %d PDFs, %d processed, %d remaining.",
             len(pdfs), len(pdfs) - len(remaining), len(remaining))

    total_articles = total_quotes = total_people = total_images = successes = 0
    zero_articles = []
    failures = []
    start_time = time.time()

    print("\n" + "=" * 90)
    print(f"{'#':<5} {'Status':<8} {'Date':<12} {'Pg':<4} {'Arts':<6} {'Quot':<6} {'Peop':<6} {'Imgs':<6} {'Newspaper'}")
    print("-" * 90)

    for idx, pdf_path in enumerate(remaining, 1):
        filename = pdf_path.name
        parsed = parse_filename(filename)
        if not parsed:
            mark_processed(conn, filename)
            continue

        newspaper, date_str, page = parsed
        search_term = get_search_term(conn, filename)

        try:
            result = process_pdf(pdf_path, docupipe_key, claude_client,
                                 newspaper, date_str, page, search_term)

            articles = result.get("articles", [])
            page_img = result.get("page_img")
            na = len(articles)
            nq = sum(len(a.get("quotes", [])) for a in articles)
            np_ = sum(len(a.get("people", [])) for a in articles)
            ni = sum(len(a.get("images", [])) for a in articles)

            store_results(conn, articles, newspaper, date_str, page, filename, page_img, search_term)
            mark_processed(conn, filename, na, search_term)

            total_articles += na
            total_quotes += nq
            total_people += np_
            total_images += ni
            successes += 1

            if na == 0:
                zero_articles.append(filename)

            status = "OK" if na > 0 else "MISS"
            print(f"{idx:<5} {status:<8} {date_str:<12} {page:<4} {na:<6} {nq:<6} {np_:<6} {ni:<6} {newspaper}")
            for a in articles:
                print(f"{'':>5}   -> {a.get('headline', '')[:70]}")

        except requests.exceptions.HTTPError as e:
            if e.response and e.response.status_code == 429:
                print(f"{idx:<5} {'RATE':<8} {date_str:<12} {page:<4}")
                time.sleep(60)
            else:
                failures.append((filename, str(e)))
                print(f"{idx:<5} {'FAIL':<8} {date_str:<12} {page:<4} {'':<6} {'':<6} {'':<6} {'':<6} {newspaper}")
                log.error("Failed: %s: %s", filename, e)

        except anthropic.RateLimitError:
            print(f"{idx:<5} {'RATE':<8} {date_str:<12} {page:<4}")
            time.sleep(60)

        except Exception as e:
            failures.append((filename, str(e)))
            print(f"{idx:<5} {'FAIL':<8} {date_str:<12} {page:<4} {'':<6} {'':<6} {'':<6} {'':<6} {newspaper}")
            log.error("Failed: %s: %s\n%s", filename, e, traceback.format_exc())

    elapsed = time.time() - start_time
    hit_rate = (successes - len(zero_articles)) / max(successes, 1) * 100

    print("\n" + "=" * 90)
    print("EXTRACTION REPORT")
    print("=" * 90)
    print(f"  Processed:   {successes}/{len(remaining)}")
    print(f"  Articles:    {total_articles}")
    print(f"  Quotes:      {total_quotes}")
    print(f"  People:      {total_people}")
    print(f"  Images:      {total_images}")
    print(f"  Hit rate:    {hit_rate:.0f}%")
    print(f"  Failures:    {len(failures)}")
    print(f"  Time:        {int(elapsed//60)}m {int(elapsed%60)}s")

    if zero_articles:
        print(f"\n  MISSED ({len(zero_articles)} pages):")
        for f in zero_articles:
            print(f"    - {f}")
    if failures:
        print(f"\n  FAILED:")
        for f, e in failures:
            print(f"    - {f}: {e}")

    db = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
    print(f"\n  DB TOTALS: {db} articles, "
          f"{conn.execute('SELECT COUNT(*) FROM quotes').fetchone()[0]} quotes, "
          f"{conn.execute('SELECT COUNT(*) FROM people').fetchone()[0]} people")
    print("=" * 90 + "\n")
    conn.close()


if __name__ == "__main__":
    main()
