"""Auto-ignore script: uses Claude Vision to classify newspaper thumbnails.

Learns from user-tagged "ignore" examples (classifieds, ads, jitney notices,
real estate listings, etc.) vs. pages that produced real articles, then
classifies unclipped pages and marks likely-ignorable ones.

Usage:
    python auto_ignore.py --year 1921                # classify 1921
    python auto_ignore.py --year 1921 --dry          # preview without changes
    python auto_ignore.py --year 1921 --batch 10     # 10 per API call (default 20)
"""

import sys
import os
import time
import sqlite3
import json
import base64
import random
import re
import logging
import argparse
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(Path(r"c:\lake_worth\.env"))

DB_PATH = r"c:\lake_worth\lake_worth.db"
THUMB_DIR = Path(r"c:\lake_worth\thumbnails")
RUNTIME_DIR = Path(r"C:\lake_worth_runtime")
STATUS_FILE = RUNTIME_DIR / "auto_ignore_status.json"
STOP_FLAG = RUNTIME_DIR / "stop_auto_ignore"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("auto_ignore")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def _write_status(data):
    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
    try:
        STATUS_FILE.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass


TRAINING_FILE = Path(r"c:\lake_worth\auto_ignore_training.json")


def _resolve_filenames(conn, filenames):
    """Given a list of pdf_filenames, return [(filename, thumb_path)] for those with valid thumbnails."""
    results = []
    for fname in filenames:
        row = conn.execute(
            "SELECT thumbnail_path FROM processed_pdfs WHERE pdf_filename = ?", (fname,)
        ).fetchone()
        if row and row["thumbnail_path"]:
            p = THUMB_DIR.parent / row["thumbnail_path"]
            if p.exists():
                results.append((fname, p))
    return results


def load_examples(conn, n_ignore=30, n_keep=30):
    """Load training examples from curated training file, with DB fallback."""
    td = None
    if TRAINING_FILE.exists():
        try:
            td = json.loads(TRAINING_FILE.read_text(encoding="utf-8"))
        except Exception as e:
            log.warning(f"Could not read training file: {e}")

    if td and (td.get("ignore_examples") or td.get("keep_examples") or td.get("hard_negatives")):
        # Use curated training set
        ignore_examples = _resolve_filenames(conn, td.get("ignore_examples", []))
        keep_examples = _resolve_filenames(conn, td.get("keep_examples", []))
        hard_negatives = _resolve_filenames(conn, td.get("hard_negatives", []))
        return ignore_examples, keep_examples, hard_negatives

    # Fallback: random DB sampling if training file is empty/missing
    log.info("No curated training data — falling back to random DB sampling")
    ignored_rows = conn.execute("""
        SELECT pdf_filename, thumbnail_path FROM processed_pdfs
        WHERE ignored = 1 AND thumbnail_path IS NOT NULL
          AND auto_ignore_confidence IS NULL
    """).fetchall()
    random.shuffle(ignored_rows)
    ignore_examples = []
    for r in ignored_rows[:n_ignore * 3]:
        p = THUMB_DIR.parent / r["thumbnail_path"]
        if p.exists():
            ignore_examples.append((r["pdf_filename"], p))
        if len(ignore_examples) >= n_ignore:
            break

    keep_rows = conn.execute("""
        SELECT DISTINCT pp.pdf_filename, pp.thumbnail_path FROM processed_pdfs pp
        JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE pp.thumbnail_path IS NOT NULL
          AND (pp.ignored IS NULL OR pp.ignored = 0)
    """).fetchall()
    random.shuffle(keep_rows)
    keep_examples = []
    for r in keep_rows[:n_keep * 3]:
        p = THUMB_DIR.parent / r["thumbnail_path"]
        if p.exists():
            keep_examples.append((r["pdf_filename"], p))
        if len(keep_examples) >= n_keep:
            break

    hard_negatives = []
    return ignore_examples, keep_examples, hard_negatives


def _img_block(path):
    """Return an image content block for the Anthropic API."""
    data = base64.standard_b64encode(path.read_bytes()).decode("ascii")
    ext = path.suffix.lower()
    media = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
    return {"type": "image", "source": {"type": "base64", "media_type": media, "data": data}}


def build_example_content(ignore_examples, keep_examples, hard_negatives):
    """Build the system message content with example images."""
    content = []
    content.append({
        "type": "text",
        "text": (
            "You are classifying newspaper page thumbnails for a Lake Worth, Texas history project. "
            "Each thumbnail is a small crop around where 'Lake Worth' was found on a newspaper page.\n\n"

            "ONLY mark IGNORE when you see CLEAR classified/listing indicators:\n"
            "- Section headers: SWAP, FOR RENT, FOR SALE, WANTED, LOST, FOUND\n"
            "- Property specs: room counts, bath numbers, phone numbers as addresses\n"
            "- Price patterns: dollar amounts, dots/dashes as price leaders\n"
            "- Dense stacked short entries (many items in a column)\n"
            "- Bus/jitney route schedules, stock tables, box scores\n"
            "- Address-as-content: 'W. Magnolia', '3111', street names listed as locations for sale/rent\n\n"

            "Mark KEEP when you see ANY of these:\n"
            "- Flowing narrative sentences (even partial ones)\n"
            "- People's names in story context (not listing context)\n"
            "- Quotes or dialogue ('said', 'declared', 'announced')\n"
            "- Event descriptions: entertained, construction, road, dam, fishing, camp\n"
            "- Photo captions or references to images\n"
            "- Headlines or story-style text\n"
            "- City commission, court, or government proceedings text\n\n"

            "CRITICAL RULE: When in doubt, ALWAYS classify as KEEP. "
            "Missing an article is far worse than clipping a classified. "
            "Only use IGNORE when you are HIGHLY CONFIDENT it is a classified or listing.\n\n"

            "Here are examples of pages correctly IGNORED (clear classifieds/listings):"
        )
    })
    for fname, path in ignore_examples:
        content.append({"type": "text", "text": f"IGNORE example: {fname}"})
        content.append(_img_block(path))

    content.append({"type": "text", "text": "\nHere are examples of pages that are KEEP (real articles, news):"})
    for fname, path in keep_examples:
        content.append({"type": "text", "text": f"KEEP example: {fname}"})
        content.append(_img_block(path))

    if hard_negatives:
        content.append({
            "type": "text",
            "text": (
                "\nIMPORTANT — TRICKY CASES: These were previously misclassified as IGNORE but "
                "are actually KEEP. They show narrative article text that can look like classifieds "
                "at first glance. Note the flowing sentences, people's names in story context, "
                "and descriptive language. These are articles, NOT classifieds:"
            )
        })
        for fname, path in hard_negatives:
            content.append({"type": "text", "text": f"KEEP (was misclassified): {fname}"})
            content.append(_img_block(path))

    return content


def classify_batch(client, example_content, batch):
    """Classify a batch of thumbnails. Returns dict {filename: 'ignore'|'keep'}."""
    user_content = []
    user_content.append({
        "type": "text",
        "text": (
            "Now classify each of the following thumbnails as IGNORE or KEEP. "
            "For each, also provide a confidence score from 0.0 to 1.0.\n"
            "Return a JSON array: [{\"filename\": \"file.pdf\", \"classification\": \"ignore\", \"confidence\": 0.95}]\n"
            "Only return the JSON, nothing else."
        )
    })
    for fname, path in batch:
        data = base64.standard_b64encode(path.read_bytes()).decode("ascii")
        ext = path.suffix.lower()
        media = "image/jpeg" if ext in (".jpg", ".jpeg") else "image/png"
        user_content.append({"type": "text", "text": fname})
        user_content.append({"type": "image", "source": {"type": "base64", "media_type": media, "data": data}})

    import re
    response = client.messages.create(
        model="claude-haiku-4-5-20251001",
        max_tokens=4096,
        messages=[
            {"role": "user", "content": example_content},
            {"role": "assistant", "content": "I understand the classification criteria. Show me the thumbnails to classify."},
            {"role": "user", "content": user_content},
        ],
    )

    result_text = response.content[0].text.strip()
    if "```" in result_text:
        m = re.search(r'```(?:json)?\s*(.*?)```', result_text, re.DOTALL)
        if m:
            result_text = m.group(1).strip()

    try:
        parsed = json.loads(result_text)
        # Normalize to dict: {filename: {"classification": ..., "confidence": ...}}
        result = {}
        if isinstance(parsed, dict):
            for k, v in parsed.items():
                if isinstance(v, str):
                    result[k] = {"classification": v, "confidence": None}
                elif isinstance(v, dict):
                    result[k] = {"classification": v.get("classification", "keep"),
                                 "confidence": v.get("confidence")}
        elif isinstance(parsed, list):
            for item in parsed:
                if isinstance(item, dict) and "filename" in item:
                    result[item["filename"]] = {
                        "classification": item.get("classification", "keep"),
                        "confidence": item.get("confidence"),
                    }
        return result
    except json.JSONDecodeError as e:
        log.warning(f"  Invalid JSON from AI: {e}")
        log.warning(f"  Response: {result_text[:200]}")
        return {}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", help="Year to classify (e.g. 1921) — shorthand for full year range")
    parser.add_argument("--months", help="Month range, e.g. 3-4 for March-April (used with --year)")
    parser.add_argument("--date-start", help="Start date YYYY-MM-DD (overrides --year)")
    parser.add_argument("--date-end", help="End date YYYY-MM-DD (overrides --year)")
    parser.add_argument("--dry", action="store_true", help="Preview without changes")
    parser.add_argument("--batch", type=int, default=20, help="Thumbnails per API call")
    args = parser.parse_args()

    if args.date_start and args.date_end:
        date_start = args.date_start
        date_end = args.date_end
        year = date_start[:4]
    elif args.year:
        year = args.year
        if args.months:
            parts = args.months.split("-")
            m_start = int(parts[0])
            m_end = int(parts[-1])
            date_start = f"{year}-{m_start:02d}-01"
            date_end = f"{year}-{m_end:02d}-31"
        else:
            date_start = f"{year}-01-01"
            date_end = f"{year}-12-31"
    else:
        parser.error("Provide --date-start and --date-end, or --year")

    if STOP_FLAG.exists():
        STOP_FLAG.unlink(missing_ok=True)

    import anthropic
    client = anthropic.Anthropic()

    conn = get_db()

    # Load training examples
    log.info("Loading training examples...")
    ignore_examples, keep_examples, hard_negatives = load_examples(conn)
    log.info(f"  {len(ignore_examples)} ignore examples, {len(keep_examples)} keep examples, {len(hard_negatives)} hard negatives")
    example_content = build_example_content(ignore_examples, keep_examples, hard_negatives)

    # Get candidates to classify
    candidates = conn.execute("""
        SELECT pdf_filename, thumbnail_path
        FROM processed_pdfs
        WHERE date_str >= ? AND date_str <= ?
          AND (clipped = 0 OR clipped IS NULL)
          AND (ignored IS NULL OR ignored = 0)
          AND thumbnail_path IS NOT NULL
        ORDER BY date_str, pdf_filename
    """, (date_start, date_end)).fetchall()

    log.info(f"Found {len(candidates)} unclipped pages in {date_start} to {date_end} to classify")
    _write_status({"status": "running", "processed": 0, "total": len(candidates),
                   "ignored": 0, "kept": 0, "failed": 0, "year": year,
                   "date_start": date_start, "date_end": date_end,
                   "dry": args.dry, "results": []})

    # Build list of (filename, path, metadata) tuples
    _fname_re = re.compile(
        r"^(?P<paper>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<page>\d+)\.pdf$",
        re.IGNORECASE
    )
    items = []
    item_meta = {}  # filename -> {date, page, newspaper, thumbnail}
    for r in candidates:
        p = THUMB_DIR.parent / r["thumbnail_path"]
        if p.exists():
            fname = r["pdf_filename"]
            items.append((fname, p))
            m = _fname_re.match(fname)
            if m:
                item_meta[fname] = {
                    "date": f"{m.group('year')}-{m.group('month')}-{m.group('day')}",
                    "page": int(m.group("page")),
                    "newspaper": m.group("paper").replace("_", " "),
                    "thumbnail": r["thumbnail_path"] or "",
                }
            else:
                item_meta[fname] = {"date": "?", "page": "?", "newspaper": fname, "thumbnail": r["thumbnail_path"] or ""}
    log.info(f"  {len(items)} have thumbnail files on disk")

    total_ignored = 0
    total_kept = 0
    total_failed = 0
    processed = 0
    ignored_list = []  # track tagged filenames for post-run review
    item_results = []  # per-item results for dry run display

    # Process in batches
    for i in range(0, len(items), args.batch):
        if STOP_FLAG.exists():
            log.info("Stop flag detected — halting")
            STOP_FLAG.unlink(missing_ok=True)
            break

        batch = items[i:i + args.batch]
        batch_num = i // args.batch + 1
        total_batches = (len(items) + args.batch - 1) // args.batch
        log.info(f"  Batch {batch_num}/{total_batches} ({len(batch)} thumbnails)")

        try:
            results = classify_batch(client, example_content, batch)
        except Exception as e:
            log.warning(f"  API error: {e}")
            total_failed += len(batch)
            processed += len(batch)
            for fname, path in batch:
                meta = item_meta.get(fname, {})
                item_results.append({"filename": fname, "classification": "error", "confidence": None,
                                     "date": meta.get("date", "?"), "page": meta.get("page", "?"),
                                     "newspaper": meta.get("newspaper", ""), "thumbnail": meta.get("thumbnail", "")})
            _write_status({"status": "running", "processed": processed, "total": len(items),
                           "ignored": total_ignored, "kept": total_kept, "failed": total_failed,
                           "year": year, "date_start": date_start, "date_end": date_end,
                           "dry": args.dry, "results": item_results})
            time.sleep(2)
            continue

        for fname, path in batch:
            info = results.get(fname, {"classification": "keep", "confidence": None})
            verdict = info["classification"].lower() if isinstance(info, dict) else str(info).lower()
            confidence = info.get("confidence") if isinstance(info, dict) else None
            conf_str = f" ({confidence:.0%})" if confidence is not None else ""
            meta = item_meta.get(fname, {})
            item_results.append({"filename": fname, "classification": verdict, "confidence": confidence,
                                 "date": meta.get("date", "?"), "page": meta.get("page", "?"),
                                 "newspaper": meta.get("newspaper", ""), "thumbnail": meta.get("thumbnail", "")})
            if verdict == "ignore":
                total_ignored += 1
                ignored_list.append(fname)
                if not args.dry:
                    conn.execute("UPDATE processed_pdfs SET ignored = 1, auto_ignore_confidence = ? WHERE pdf_filename = ?",
                                 (confidence, fname))
                log.info(f"    IGNORE{conf_str}: {fname}")
            else:
                total_kept += 1
                if not args.dry and confidence is not None:
                    conn.execute("UPDATE processed_pdfs SET auto_ignore_confidence = ? WHERE pdf_filename = ?",
                                 (confidence, fname))

        if not args.dry:
            conn.commit()

        processed += len(batch)
        _write_status({"status": "running", "processed": processed, "total": len(items),
                       "ignored": total_ignored, "kept": total_kept, "failed": total_failed,
                       "year": year, "date_start": date_start, "date_end": date_end,
                       "dry": args.dry, "results": item_results})
        time.sleep(0.5)

    status = "done_dry" if args.dry else "done"
    _write_status({"status": status, "processed": processed, "total": len(items),
                   "ignored": total_ignored, "kept": total_kept, "failed": total_failed,
                   "year": year, "date_start": date_start, "date_end": date_end,
                   "dry": args.dry, "results": item_results})

    # Save manifest of tagged files for false-positive review
    manifest_file = RUNTIME_DIR / f"auto_ignore_manifest_{year}_{date_start}_{date_end}.json"
    try:
        manifest_file.write_text(json.dumps({
            "date_start": date_start, "date_end": date_end,
            "run_time": time.strftime("%Y-%m-%d %H:%M:%S"),
            "ignored": ignored_list,
            "total_processed": processed,
        }, indent=2), encoding="utf-8")
        log.info(f"Manifest saved: {manifest_file}")
    except Exception as e:
        log.warning(f"Could not save manifest: {e}")

    log.info(f"\nDone: {total_ignored} ignored, {total_kept} kept, {total_failed} failed out of {len(items)}")
    conn.close()


if __name__ == "__main__":
    main()
