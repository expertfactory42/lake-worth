"""
Standalone Librarian server for the Lake Worth archive project.
Serves librarian.html and provides API endpoints to browse enriched articles.

Usage: python librarian_server.py
Then open http://localhost:1212 in your browser.
"""

import json
import os
import signal
import sqlite3
import subprocess
import sys
import urllib.parse
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

import anthropic

BASE_DIR = Path(r"C:\lake_worth")
DB_PATH = BASE_DIR / "lake_worth.db"
PORT = 1212
CORRECTIONS_FILE = BASE_DIR / "corrections.sql"
PID_FILE = BASE_DIR / "enrichment_pid.json"


def _start_enrichment_proc(from_date=None, to_date=None, limit=None, rerun=False):
    cmd = [sys.executable, str(BASE_DIR / "enrich_articles.py")]
    if from_date:
        cmd += ["--from-date", from_date]
    if to_date:
        cmd += ["--to-date", to_date]
    if limit:
        cmd += ["--limit", str(int(limit))]
    if rerun:
        cmd.append("--rerun")
    proc = subprocess.Popen(cmd, cwd=str(BASE_DIR))
    PID_FILE.write_text(json.dumps({"pid": proc.pid}))
    return proc.pid


def _is_process_running(pid):
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True
        )
        return str(pid) in result.stdout and "python" in result.stdout.lower()
    except Exception:
        return False


def _stop_enrichment_proc():
    if not PID_FILE.exists():
        return False, "No PID file found"
    try:
        data = json.loads(PID_FILE.read_text())
        pid = data.get("pid")
        os.kill(pid, signal.SIGTERM)
        PID_FILE.unlink(missing_ok=True)
        (BASE_DIR / "enrichment_status.json").write_text(json.dumps({"active_agent": None}))
        return True, f"Sent SIGTERM to PID {pid}"
    except ProcessLookupError:
        PID_FILE.unlink(missing_ok=True)
        return False, "Process not found (already stopped)"
    except Exception as e:
        return False, str(e)


def _parse_correction_statements(sql: str) -> list:
    statements = []
    buf = []
    for line in sql.splitlines():
        stripped = line.strip()
        if stripped.startswith("--") or not stripped:
            continue
        buf.append(line)
        if stripped.endswith(";"):
            stmt = "\n".join(buf).strip()
            if stmt:
                statements.append(stmt)
            buf = []
    return statements


def _apply_corrections():
    if not CORRECTIONS_FILE.exists():
        return 0, 0, []
    sql = CORRECTIONS_FILE.read_text(encoding="utf-8")
    statements = _parse_correction_statements(sql)
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    applied = 0
    failed = 0
    results = []
    for stmt in statements:
        try:
            cur = conn.execute(stmt)
            conn.commit()
            applied += 1
            results.append({"ok": True, "rows": cur.rowcount, "stmt": stmt[:80]})
        except Exception as e:
            failed += 1
            results.append({"ok": False, "error": str(e), "stmt": stmt[:80]})
    conn.close()
    return applied, failed, results


# ---------------------------------------------------------------------------
# Corrections AI chat
# ---------------------------------------------------------------------------

_CORRECTIONS_SYSTEM = """\
You are a data quality assistant for the Lake Worth historical newspaper archive (Fort Worth Star-Telegram, 1909–1925).

Your job: help the user identify and fix data errors in the database — especially OCR misreadings of names, places, and organizations.

Database structure relevant to corrections:
- entities table: canonical_name (the field to fix), name, type
- article_people table: mention_text (people named in articles), role

The corrections.sql file holds idempotent SQL UPDATE statements. Each time it is applied, it re-applies all fixes. So corrections must be safe to run multiple times (UPDATE ... WHERE ... is fine).

Rules:
- ONLY update canonical_name in entities, or mention_text in article_people
- NEVER suggest modifying full_text, quote_text, or any verbatim archival fields
- Always end SQL statements with a semicolon
- Add a comment above each new correction: -- YYYY-MM-DD: description

Workflow when a user reports an error:
1. Search the DB to confirm the error and see how many rows are affected
2. Read corrections.sql to see what's already there
3. Write an updated corrections.sql with the new fix appended
4. Apply corrections to the DB
5. Report what you found, what you fixed, and how many rows were affected\
"""

_CORRECTIONS_TOOLS = [
    {
        "name": "search_db",
        "description": "Search the database for entity or person names matching a pattern. Use this to verify errors and see how widespread they are.",
        "input_schema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Partial name to search for"},
                "table": {"type": "string", "enum": ["entities", "people"],
                          "description": "'entities' for places/orgs/lakes/etc, 'people' for person names"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "read_corrections",
        "description": "Read the current corrections.sql file.",
        "input_schema": {"type": "object", "properties": {}}
    },
    {
        "name": "write_corrections",
        "description": "Overwrite corrections.sql with new content. Always include the full file — existing corrections plus any new ones.",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "Full content of corrections.sql"}
            },
            "required": ["content"]
        }
    },
    {
        "name": "apply_corrections",
        "description": "Apply all corrections in corrections.sql to the live database.",
        "input_schema": {"type": "object", "properties": {}}
    }
]


def _execute_correction_tool(name: str, inputs: dict) -> object:
    if name == "search_db":
        query = inputs.get("query", "")
        table = inputs.get("table", "entities")
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            if table == "people":
                rows = conn.execute(
                    "SELECT mention_text, role, COUNT(*) as count "
                    "FROM article_people WHERE mention_text LIKE ? "
                    "GROUP BY mention_text ORDER BY count DESC LIMIT 20",
                    (f"%{query}%",)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT e.canonical_name, e.type, COUNT(ae.article_id) as mentions "
                    "FROM entities e LEFT JOIN article_entities ae ON ae.entity_id = e.id "
                    "WHERE e.canonical_name LIKE ? OR e.name LIKE ? "
                    "GROUP BY e.canonical_name ORDER BY mentions DESC LIMIT 20",
                    (f"%{query}%", f"%{query}%")
                ).fetchall()
            result = [dict(r) for r in rows]
        except Exception as e:
            result = {"error": str(e)}
        conn.close()
        return result

    elif name == "read_corrections":
        if CORRECTIONS_FILE.exists():
            return {"content": CORRECTIONS_FILE.read_text(encoding="utf-8")}
        return {"content": "-- No corrections yet\n"}

    elif name == "write_corrections":
        content = inputs.get("content", "")
        CORRECTIONS_FILE.write_text(content, encoding="utf-8")
        return {"ok": True}

    elif name == "apply_corrections":
        applied, failed, results = _apply_corrections()
        return {"applied": applied, "failed": failed, "results": results}

    return {"error": f"Unknown tool: {name}"}


def _run_corrections_chat(messages: list) -> dict:
    client = anthropic.Anthropic()
    claude_messages = [{"role": m["role"], "content": m["content"]} for m in messages]
    actions = []

    while True:
        response = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            system=_CORRECTIONS_SYSTEM,
            tools=_CORRECTIONS_TOOLS,
            messages=claude_messages
        )

        text_parts = []
        tool_calls = []
        for block in response.content:
            if block.type == "text":
                text_parts.append(block.text)
            elif block.type == "tool_use":
                tool_calls.append(block)

        if response.stop_reason != "tool_use" or not tool_calls:
            return {"reply": "\n".join(text_parts), "actions": actions}

        claude_messages.append({"role": "assistant", "content": response.content})

        tool_results = []
        for tc in tool_calls:
            result = _execute_correction_tool(tc.name, tc.input)
            actions.append({"tool": tc.name})
            tool_results.append({
                "type": "tool_result",
                "tool_use_id": tc.id,
                "content": json.dumps(result)
            })
        claude_messages.append({"role": "user", "content": tool_results})


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    pass


class LibrarianHandler(SimpleHTTPRequestHandler):
    def log_message(self, format, *args):
        pass  # suppress request logging

    def do_GET(self):
        if self.path == "/" or self.path == "/librarian.html":
            fpath = BASE_DIR / "librarian.html"
            if fpath.exists():
                self.send_response(200)
                self.send_header("Content-type", "text/html; charset=utf-8")
                self.end_headers()
                with open(fpath, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_error(404, "librarian.html not found")
        elif self.path == "/api/librarian/stats":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                row = conn.execute("""
                    SELECT
                        (SELECT COUNT(*) FROM articles) as total_articles,
                        (SELECT COUNT(DISTINCT article_id) FROM article_type) as enriched,
                        (SELECT COUNT(*) FROM enrichment_runs) as total_runs,
                        (SELECT COUNT(*) FROM eras) as total_eras
                """).fetchone()
                conn.close()
                data = {
                    "total_articles": row["total_articles"],
                    "enriched": row["enriched"],
                    "pending": row["total_articles"] - row["enriched"],
                    "total_runs": row["total_runs"],
                    "total_eras": row["total_eras"],
                }
                self.wfile.write(json.dumps(data).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/librarian/articles"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                parsed = urllib.parse.urlparse(self.path)
                qs = urllib.parse.parse_qs(parsed.query)
                page = int(qs.get("page", ["1"])[0])
                per_page = int(qs.get("per_page", ["50"])[0])
                era_slug = qs.get("era", [None])[0]
                art_type = qs.get("type", [None])[0]
                status = qs.get("status", ["all"])[0]
                search = qs.get("search", [None])[0]
                sort_dir = "ASC" if qs.get("sort", ["desc"])[0] == "asc" else "DESC"
                offset_val = (page - 1) * per_page

                where_clauses = []
                params = []

                if era_slug:
                    pass  # era filter removed
                if art_type:
                    where_clauses.append("at.type = ?")
                    params.append(art_type)
                if status == "enriched":
                    where_clauses.append("at.article_id IS NOT NULL")
                elif status == "pending":
                    where_clauses.append("at.article_id IS NULL")
                if search:
                    where_clauses.append("a.headline LIKE ?")
                    params.append(f"%{search}%")

                where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""

                count_params = list(params)
                count_sql = f"""
                    SELECT COUNT(DISTINCT a.id)
                    FROM articles a
                    LEFT JOIN article_type at ON at.article_id = a.id
                    LEFT JOIN article_flags af ON af.article_id = a.id
                    {where_sql}
                """

                list_sql = f"""
                    SELECT
                        a.id,
                        a.date,
                        a.headline,
                        a.newspaper,
                        a.page,
                        SUBSTR(a.full_text, 1, 200) as snippet,
                        at.type,
                        CASE WHEN at.article_id IS NOT NULL THEN 1 ELSE 0 END as enriched
                    FROM articles a
                    LEFT JOIN article_type at ON at.article_id = a.id
                    LEFT JOIN article_flags af ON af.article_id = a.id
                    {where_sql}
                    GROUP BY a.id
                    ORDER BY a.date {sort_dir}
                    LIMIT ? OFFSET ?
                """

                conn = get_db()
                total = conn.execute(count_sql, count_params).fetchone()[0]
                list_params = list(params) + [per_page, offset_val]
                rows = conn.execute(list_sql, list_params).fetchall()
                conn.close()

                articles_out = []
                for r in rows:
                    articles_out.append({
                        "id": r["id"],
                        "date": r["date"],
                        "headline": r["headline"],
                        "newspaper": r["newspaper"],
                        "page": r["page"],
                        "snippet": r["snippet"],
                        "type": r["type"],
                        "enriched": bool(r["enriched"]),
                    })

                self.wfile.write(json.dumps({
                    "articles": articles_out,
                    "total": total,
                    "page": page,
                    "per_page": per_page,
                    "pages": max(1, (total + per_page - 1) // per_page),
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/librarian/article/"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                article_id = int(self.path.split("/api/librarian/article/")[1].split("?")[0])
                conn = get_db()

                row = conn.execute("""
                    SELECT
                        a.id, a.date, a.headline, a.newspaper, a.page,
                        a.full_text, a.has_photo, a.photo_description,
                        at.type, at.confidence as type_confidence, at.summary,
                        at.enrichment_run_id,
                        en_run.id as run_id, en_run.agent_name, en_run.model,
                        en_run.started_at as run_started_at
                    FROM articles a
                    LEFT JOIN article_type at ON at.article_id = a.id
                    LEFT JOIN enrichment_runs en_run ON en_run.id = at.enrichment_run_id
                    WHERE a.id = ?
                """, (article_id,)).fetchone()

                if not row:
                    self.wfile.write(json.dumps({"error": "Article not found"}).encode())
                    conn.close()
                    return

                quotes = conn.execute("""
                    SELECT id, quote_text, speaker, speaker_role, context
                    FROM quotes WHERE article_id = ?
                """, (article_id,)).fetchall()

                entities = conn.execute("""
                    SELECT en.name, en.type, ae.mention_text, ae.verified
                    FROM article_entities ae
                    JOIN entities en ON en.id = ae.entity_id
                    WHERE ae.article_id = ?
                """, (article_id,)).fetchall()

                # People extracted from this article
                people = conn.execute("""
                    SELECT mention_text, role FROM article_people
                    WHERE article_id = ?
                """, (article_id,)).fetchall()

                # Measurements extracted from this article
                measurements = conn.execute("""
                    SELECT value, unit, context FROM measurements
                    WHERE article_id = ?
                    ORDER BY id
                """, (article_id,)).fetchall()

                # Key points extracted from this article
                key_points = conn.execute("""
                    SELECT point FROM article_key_points
                    WHERE article_id = ?
                    ORDER BY sort_order
                """, (article_id,)).fetchall()

                # Flags for this article
                flags_row = conn.execute("""
                    SELECT has_map, has_photo_ref, has_illustration, notable_quote,
                           tragic, has_conflict, tone
                    FROM article_flags WHERE article_id = ?
                """, (article_id,)).fetchone()

                conn.close()

                enrichment_run = None
                if row["run_id"]:
                    enrichment_run = {
                        "id": row["run_id"],
                        "agent_name": row["agent_name"],
                        "model": row["model"],
                        "started_at": row["run_started_at"],
                    }

                self.wfile.write(json.dumps({
                    "id": row["id"],
                    "date": row["date"],
                    "headline": row["headline"],
                    "newspaper": row["newspaper"],
                    "page": row["page"],
                    "full_text": row["full_text"],
                    "has_photo": row["has_photo"],
                    "photo_description": row["photo_description"],
                    "type": row["type"],
                    "type_confidence": row["type_confidence"],
                    "summary": row["summary"],
                    "entities": [{"name": e["name"], "type": e["type"], "mention_text": e["mention_text"], "verified": e["verified"]} for e in entities],
                    "quotes": [{"id": q["id"], "quote_text": q["quote_text"], "speaker": q["speaker"], "speaker_role": q["speaker_role"], "context": q["context"]} for q in quotes],
                    "people": [{"name": p["mention_text"], "role": p["role"]} for p in people],
                    "measurements": [{"value": m["value"], "unit": m["unit"], "context": m["context"]} for m in measurements],
                    "key_points": [r["point"] for r in key_points],
                    "flags": dict(flags_row) if flags_row else {},
                    "enrichment_run": enrichment_run,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/librarian/runs":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                rows = conn.execute("""
                    SELECT id, agent_name, prompt_version, prompt_hash, model,
                           started_at, completed_at, articles_processed, notes
                    FROM enrichment_runs
                    ORDER BY started_at DESC
                """).fetchall()
                conn.close()
                self.wfile.write(json.dumps([dict(r) for r in rows]).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/librarian/agent_status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import os
                status_path = os.path.join(os.path.dirname(__file__), "enrichment_status.json")
                if os.path.exists(status_path):
                    with open(status_path, "r", encoding="utf-8") as f:
                        self.wfile.write(f.read().encode())
                else:
                    # Check enrichment_runs for any in-progress run (started but not completed)
                    conn = get_db()
                    active = conn.execute("""
                        SELECT id, agent_name, model, started_at, articles_processed
                        FROM enrichment_runs
                        WHERE started_at IS NOT NULL AND completed_at IS NULL
                        ORDER BY started_at DESC LIMIT 1
                    """).fetchone()
                    conn.close()
                    if active:
                        self.wfile.write(json.dumps({
                            "active_agent": dict(active).get("agent_name"),
                            "run_id": dict(active).get("id"),
                            "articles_done": dict(active).get("articles_processed", 0),
                            "current_articles": []
                        }).encode())
                    else:
                        self.wfile.write(json.dumps({"active_agent": None, "current_articles": []}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e), "active_agent": None, "current_articles": []}).encode())

        elif self.path == "/api/librarian/agents":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import os
                config_path = os.path.join(os.path.dirname(__file__), "agents_config.json")
                with open(config_path, "r", encoding="utf-8") as f:
                    self.wfile.write(f.read().encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/librarian/corrections":
            self.send_response(200)
            self.send_header("Content-type", "text/plain; charset=utf-8")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                if CORRECTIONS_FILE.exists():
                    self.wfile.write(CORRECTIONS_FILE.read_bytes())
                else:
                    self.wfile.write(b"-- No corrections yet\n")
            except Exception as e:
                self.wfile.write(f"-- Error: {e}\n".encode())

        elif self.path == "/api/librarian/articles_by_year":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                rows = conn.execute("""
                    SELECT
                        SUBSTR(a.date, 1, 4) as year,
                        COUNT(*) as total,
                        COUNT(at.article_id) as enriched
                    FROM articles a
                    LEFT JOIN article_type at ON at.article_id = a.id
                    WHERE a.date IS NOT NULL AND a.date != ''
                    GROUP BY year
                    ORDER BY year
                """).fetchall()
                conn.close()
                self.wfile.write(json.dumps([dict(r) for r in rows]).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/librarian/last_enriched_date":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                row = conn.execute("""
                    SELECT MAX(a.date) as last_date
                    FROM articles a
                    JOIN article_type at ON at.article_id = a.id
                """).fetchone()
                conn.close()
                self.wfile.write(json.dumps({"last_date": row["last_date"]}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        else:
            self.send_error(404, "Not found")

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        raw_body = self.rfile.read(content_length) if content_length > 0 else b""
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.end_headers()
        try:
            if self.path == "/api/librarian/run/start":
                params = json.loads(raw_body.decode()) if raw_body else {}
                if PID_FILE.exists():
                    try:
                        data = json.loads(PID_FILE.read_text())
                        pid = data.get("pid")
                        if pid:
                            if _is_process_running(pid):
                                self.wfile.write(json.dumps({"error": f"Already running (PID {pid})"}).encode())
                                return
                            PID_FILE.unlink(missing_ok=True)
                    except Exception:
                        PID_FILE.unlink(missing_ok=True)
                pid = _start_enrichment_proc(
                    from_date=params.get("from_date"),
                    to_date=params.get("to_date"),
                    limit=params.get("limit"),
                    rerun=bool(params.get("rerun"))
                )
                self.wfile.write(json.dumps({"ok": True, "pid": pid}).encode())
            elif self.path == "/api/librarian/run/stop":
                ok, msg = _stop_enrichment_proc()
                self.wfile.write(json.dumps({"ok": ok, "message": msg}).encode())
            elif self.path == "/api/librarian/corrections/save":
                text = raw_body.decode("utf-8")
                CORRECTIONS_FILE.write_text(text, encoding="utf-8")
                self.wfile.write(json.dumps({"ok": True}).encode())
            elif self.path == "/api/librarian/corrections/apply":
                applied, failed, results = _apply_corrections()
                self.wfile.write(json.dumps({
                    "ok": True,
                    "message": f"{applied} applied, {failed} failed",
                    "applied": applied,
                    "failed": failed,
                    "results": results
                }).encode())
            elif self.path == "/api/librarian/corrections/chat":
                params = json.loads(raw_body.decode()) if raw_body else {}
                messages = params.get("messages", [])
                result = _run_corrections_chat(messages)
                self.wfile.write(json.dumps(result).encode())
            else:
                self.wfile.write(json.dumps({"error": "Not found"}).encode())
        except Exception as e:
            self.wfile.write(json.dumps({"error": str(e)}).encode())


if __name__ == '__main__':
    server = ThreadedHTTPServer(("", PORT), LibrarianHandler)
    print(f"Librarian running at http://localhost:{PORT}")
    server.serve_forever()
