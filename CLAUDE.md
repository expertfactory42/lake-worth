# Lake Worth Newspaper Archive Project

Historical newspaper archive project (Fort Worth Star-Telegram, 1909-1925). Goal: books, web stories, and videos from extracted articles. Emphasis on preserving exact quotes.

## Key Apps

- **clip_and_extract.py** — Main clipper. Visits unclipped pages on Newspapers.com, clips full pages, scrapes OCR text, extracts articles via Claude Haiku. Run via `run_clipper_loop.py`.
- **collect_urls.py** — URL collector. Gathers page URLs from Newspapers.com search results. Multi-instance via dashboard.
- **browser_session.py** — Shared browser/login/account utilities used by both apps.
- **dashboard_server.py** / **dashboard.html** — Web dashboard for monitoring and controlling clipper and collector instances.
- **extract_articles.py** — Article extraction from OCR text.
- **download_newspapers.py** / **download_scheduler.py** — PDF download pipeline.

## Database: lake_worth.db

SQLite database with extracted articles, quotes, people, processing state. Took significant time and API credits to build.

## Rules — NO EXCEPTIONS

### 1. Ask Before Doing
Always present the plan and wait for approval before:
- Editing code
- Running git commands (stash, checkout, reset, etc.)
- Modifying the database
- Undoing previous changes

### 2. Never Destroy Database Content
- NEVER delete columns, drop tables, delete rows, clear/reset/truncate data
- NEVER run git operations that could affect the database file
- Only ADD columns, ADD data, UPDATE existing data when instructed

### 3. Don't Break Working Code
- Don't rewrite working code carelessly
- Don't modify logic that wasn't asked to be changed
- Don't add "improvements", refactors, or cleanup beyond what was requested
- Don't change variable names, reorder code, or touch anything outside scope

### 4. Mandatory Verification Agent After Edits
After EVERY code edit, before telling the user it's done, launch a verification Agent that:
1. Runs `git diff` on all changed files
2. Checks EVERY hunk against what the user actually asked for
3. Flags any change that:
   - Modifies logic the user didn't ask to change
   - Removes conditions, parameters, or SQL clauses from working code
   - Adds "improvements", refactors, or cleanup beyond requested scope
   - Changes variable names, reorders code, or touches anything not requested
4. If flagged issues found: REVERT the unintended changes before proceeding
5. Report the review results to the user

This is non-negotiable. Use the Agent tool with a dedicated review prompt every time.

### 5. Newspapers.com Suspected Limits
- ~300 clips per account per day before throttling risk
- ~300 URLs gathered per account per day before throttling risk
- ~24 hours cooldown between runs
- Cancellation risk suspected at 750+ per day (unconfirmed)
- These are observed/suspected limits, not confirmed by Newspapers.com
- Accounts are shared across apps (clipper and collector)
- Account error handling is being unified (see memory: project_universal_account_errors.md)
