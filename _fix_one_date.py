"""One-off helper: run fix_missing_urls.py's logic for a single (DATE, TERM).

Purpose
-------
fix_missing_urls.py walks ALL dates that have rows in processed_pdfs with
NULL url, in ascending order. If you just want to test the fixer on one
specific date (or re-run it for a single date that failed), edit DATE/TERM
below and run this script. It imports setup_driver / search_date /
download_thumbnails_batch from fix_missing_urls and does exactly the same
work that the main script would do for that one group.

Usage
-----
    1. Edit DATE and TERM below.
    2. python _fix_one_date.py

First used 2026-04-07 to verify the fixer on 1919-06-01 (19 rows) before
running the full backfill for 166 missing URLs.
"""
import sys, os
sys.path.insert(0, os.path.dirname(__file__))
from fix_missing_urls import (
    get_db, setup_driver, search_date, download_thumbnails_batch, log
)

DATE = "1919-06-01"
TERM = "lake worth"

conn = get_db()
needed = {r["pdf_filename"] for r in conn.execute(
    "SELECT pdf_filename FROM processed_pdfs WHERE date_str=? AND (url IS NULL OR url='') AND (ignored IS NULL OR ignored=0)",
    (DATE,)
)}
log.info(f"Need {len(needed)} entries for {DATE}")

driver = setup_driver()
try:
    results = search_date(driver, DATE, TERM, needed_filenames=needed)
    log.info(f"Search returned {len(results)} results")

    url_lookup = {r["pdf_filename"]: r for r in results}
    to_dl = [{"pdf_filename": f, "thumb_url": url_lookup[f].get("thumb_url", "")}
             for f in needed if f in url_lookup]
    thumbs = download_thumbnails_batch(driver, to_dl)

    matched = 0
    for f in needed:
        if f in url_lookup:
            conn.execute(
                "UPDATE processed_pdfs SET url=?, thumbnail_path=? WHERE pdf_filename=?",
                (url_lookup[f]["url"], thumbs.get(f) or None, f),
            )
            matched += 1
    conn.commit()
    log.info(f"Matched {matched}/{len(needed)}")
finally:
    try: driver.quit()
    except Exception: pass
    conn.close()
