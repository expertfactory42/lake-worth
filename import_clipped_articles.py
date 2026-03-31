"""Re-run AI extraction on clipped pages that have OCR text but no articles."""

import os
import sys
import sqlite3
import re
import time
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), '.env'))

# Import from clip_and_extract
from clip_and_extract import (
    DB_PATH, SEARCH_TERM, extract_articles_with_ai, save_articles,
    ensure_columns, log
)

def main():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    ensure_columns(conn)

    # Find clipped pages from today's run (1918-05-06 onward) with OCR but no articles
    rows = conn.execute("""
        SELECT pp.pdf_filename, pp.ocr_text, pp.clip_url
        FROM processed_pdfs pp
        WHERE pp.clipped = 1
          AND pp.ocr_text IS NOT NULL
          AND length(pp.ocr_text) > 100
          AND pp.pdf_filename >= 'Fort_Worth_Record_Telegram_1918_05_06'
          AND NOT EXISTS (
              SELECT 1 FROM articles a WHERE a.pdf_filename = pp.pdf_filename
          )
        ORDER BY pp.pdf_filename
    """).fetchall()

    print(f"Found {len(rows)} clipped pages needing AI extraction")

    total_articles = 0
    processed = 0
    skipped = 0

    for row in rows:
        pdf_filename = row["pdf_filename"]
        ocr_text = row["ocr_text"]
        clip_url = row["clip_url"] or ""

        # Parse date/newspaper/page from filename
        m = re.search(r'(.+?)_(\d{4})_(\d{2})_(\d{2})_(\d+)\.pdf$', pdf_filename)
        if not m:
            skipped += 1
            continue

        newspaper = m.group(1).replace("_", " ")
        date_str = f"{m.group(2)}-{m.group(3)}-{m.group(4)}"
        page = int(m.group(5))

        processed += 1
        articles = extract_articles_with_ai(ocr_text, date_str, newspaper, page)

        if articles:
            count = save_articles(conn, pdf_filename, articles, SEARCH_TERM, clip_url)
            total_articles += count
            print(f"  [{processed}/{len(rows)}] {pdf_filename}: {count} articles")
        else:
            if processed % 50 == 0:
                print(f"  [{processed}/{len(rows)}] {pdf_filename}: no Lake Worth mentions")

    print(f"\nDone! Processed {processed}, found {total_articles} articles, skipped {skipped}")
    conn.close()

if __name__ == "__main__":
    main()
