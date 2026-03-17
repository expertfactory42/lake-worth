"""
Simple HTTP server that serves the dashboard and provides API endpoints
to read from the SQLite database. Auto-refreshes as extraction runs.

Usage: python dashboard_server.py
Then open http://localhost:8765 in your browser.
"""

import json
import sqlite3
import os
import glob
from http.server import HTTPServer, SimpleHTTPRequestHandler
from pathlib import Path

BASE_DIR = Path(r"C:\lake_worth")
DB_PATH = BASE_DIR / "lake_worth.db"
PDF_DIR = BASE_DIR / "pdfs"
PORT = 8765


def get_db():
    conn = sqlite3.connect(str(DB_PATH))
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
        "SELECT * FROM articles ORDER BY date, page"
    ).fetchall()

    article_list = []
    for a in article_rows:
        a_dict = dict(a)
        a_quotes = conn.execute(
            "SELECT * FROM quotes WHERE article_id = ?", (a["id"],)
        ).fetchall()
        a_dict["quotes"] = [dict(q) for q in a_quotes]
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

    conn.close()

    return {
        "stats": stats,
        "articles": article_list,
        "quotes": quote_list,
        "people": people_list,
        "log": log_list,
    }


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
        else:
            super().do_GET()

    def log_message(self, format, *args):
        pass  # Suppress request logging


def main():
    server = HTTPServer(("localhost", PORT), DashboardHandler)
    print(f"Dashboard running at http://localhost:{PORT}")
    print("Press Ctrl+C to stop")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == "__main__":
    main()
