## PDF Download Automation (Archived 2026-03-18)

These scripts automate downloading newspaper pages as PDFs from newspapers.com
using Selenium/undetected-chromedriver.

### Files
- `download_newspapers.py` — Chrome automation that searches newspapers.com,
  navigates to each result, and downloads pages as PDFs via the Print/Save flow.
- `download_scheduler.py` — Hourly scheduler wrapper that runs downloads in
  batches of 60 (respecting rate limits) and keeps the computer awake.

### Why archived
Replaced by a manual clip-based workflow:
1. `collect_search_results.py` collects metadata (date, page, URL) without downloading
2. User clicks WWW to view on newspapers.com, clips the article
3. Drag-and-drop clip URL into dashboard imports OCR text automatically

### To restore
Copy these files back to `c:\lake_worth\` and run:
```
python download_scheduler.py
```
Requires: undetected-chromedriver, selenium, Chrome browser, newspapers.com login.
