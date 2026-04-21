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
import sys
import glob
import subprocess
import signal
import urllib.request
import urllib.error
from http.server import HTTPServer, SimpleHTTPRequestHandler
from socketserver import ThreadingMixIn
from pathlib import Path

from datetime import datetime, timedelta

BASE_DIR = Path(r"C:\lake_worth")
RUNTIME_DIR = Path(r"C:\lake_worth_runtime")
DB_PATH = BASE_DIR / "lake_worth.db"
PDF_DIR = BASE_DIR / "pdfs"
LOG_DIR = BASE_DIR / "collector_logs"
BOOK_NOTES_PATH = BASE_DIR / "book_notes.txt"
STOP_FLAG = RUNTIME_DIR / "stop_clipper"
PORT = 8765


def get_db():
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.row_factory = sqlite3.Row
    return conn


_NO_WINDOW = getattr(subprocess, "CREATE_NO_WINDOW", 0)


_LOG_TS_RE = re.compile(r'^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')


def _parse_log_ts(line):
    m = _LOG_TS_RE.match(line)
    if not m:
        return None
    try:
        return datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S")
    except Exception:
        return None


def _determine_run_set_start():
    """Return a datetime marking the start of the most recent 'run set'.

    A run set is the cluster of clipper_instances.started_at values around
    the most recent activity. If any instance has a heartbeat within the
    last 5 minutes, the run set starts at the earliest started_at among
    currently-active instances. Otherwise we cluster backward from the
    latest started_at, grouping any rows within 15 minutes of each other.
    Returns None if there is no usable data.
    """
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=30)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT started_at, heartbeat_at FROM clipper_instances "
            "WHERE started_at IS NOT NULL"
        ).fetchall()
        conn.close()
    except Exception:
        return None
    if not rows:
        return None

    def _p(s):
        if not s:
            return None
        try:
            return datetime.strptime(s[:19], "%Y-%m-%d %H:%M:%S")
        except Exception:
            return None

    now = datetime.now()
    recent_threshold = now - timedelta(minutes=5)
    active_starts = []
    for r in rows:
        hb = _p(r["heartbeat_at"])
        st = _p(r["started_at"])
        if hb and st and hb >= recent_threshold:
            active_starts.append(st)
    if active_starts:
        return min(active_starts)

    starts = sorted([_p(r["started_at"]) for r in rows if _p(r["started_at"])])
    if not starts:
        return None
    cluster_start = starts[-1]
    gap = timedelta(minutes=15)
    for s in reversed(starts[:-1]):
        if cluster_start - s <= gap:
            cluster_start = s
        else:
            break
    return cluster_start


_daily_stats_cache = {"ts": 0.0, "out": None}
_DAILY_STATS_TTL = 10.0  # seconds


def _scan_logs_since(since_dt):
    """Scan clipper_*.log files for events on/after since_dt. Returns
    dict(clipped=int, errors=int).
    """
    result = {"clipped": 0, "errors": 0}
    if since_dt is None:
        return result
    try:
        since_ts = since_dt.timestamp()
        files = glob.glob(str(LOG_DIR / "clipper_*.log"))
        # Only scan slot-specific logs (clipper_slot*) when they exist.
        # The base clipper_YYYYMMDD.log used to be a duplicate of the slot
        # file; scanning both would double-count events.
        slot_files = [f for f in files if "clipper_slot" in os.path.basename(f)]
        if slot_files:
            files = slot_files
        files = [f for f in files if os.path.getmtime(f) >= since_ts - 3600]
        # Only tail the last 512KB of each file to avoid scanning megabytes
        # of clipper log on every poll.
        TAIL_BYTES = 512 * 1024
        for fp in files:
            try:
                size = os.path.getsize(fp)
                with open(fp, "rb") as fh:
                    if size > TAIL_BYTES:
                        fh.seek(size - TAIL_BYTES)
                        fh.readline()  # discard partial line
                    data = fh.read()
                text = data.decode("utf-8", errors="replace")
                for line in text.splitlines():
                    ts = _parse_log_ts(line)
                    if ts is None or ts < since_dt:
                        continue
                    if "Clicked Save button" in line:
                        result["clipped"] += 1
                    if "[ERROR]" in line:
                        result["errors"] += 1
            except Exception:
                continue
    except Exception:
        pass
    return result


def _count_articles_since(conn, since_dt):
    """Returns (articles_total, picture_heavy) counted from the articles
    table where created_at >= since_dt. If since_dt is None, returns zeros.
    """
    if since_dt is None:
        return 0, 0
    since_s = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    try:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM articles WHERE created_at >= ?",
            (since_s,),
        ).fetchone()["c"]
        picture = conn.execute(
            "SELECT COUNT(*) AS c FROM articles "
            "WHERE created_at >= ? AND headline LIKE 'PICTURE HEAVY%'",
            (since_s,),
        ).fetchone()["c"]
        return total, picture
    except Exception:
        return 0, 0


def _current_queue_size(conn):
    try:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM processed_pdfs "
            "WHERE (clipped = 0 OR clipped IS NULL) "
            "AND (ignored IS NULL OR ignored = 0) "
            "AND url IS NOT NULL AND url != ''"
        ).fetchone()
        return row["c"] if row else 0
    except Exception:
        return 0


def _is_clipper_running():
    """Check if clip_and_extract.py is running."""
    try:
        result = subprocess.run(
            ["wmic", "process", "where", "name='python.exe'", "get", "CommandLine"],
            capture_output=True, text=True, timeout=5,
            creationflags=_NO_WINDOW,
        )
        return "clip_and_extract" in result.stdout
    except Exception:
        return False


# === MULTI-INSTANCE SUPPORT ===

GLOBAL_STOP_FLAG = RUNTIME_DIR / "stop_clipper_all"


def _instance_stop_flag(slot_id):
    return RUNTIME_DIR / f"stop_clipper_{slot_id}"


def _pid_alive(pid):
    """Return True if the given PID is currently running. Uses the Windows
    kernel32 OpenProcess API for an instant check — no subprocess overhead.
    Falls back to tasklist only if the ctypes call fails unexpectedly."""
    if not pid:
        return False
    try:
        pid = int(pid)
    except (TypeError, ValueError):
        return False
    # Fast path: kernel32.OpenProcess with PROCESS_QUERY_LIMITED_INFORMATION
    try:
        import ctypes
        PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
        handle = ctypes.windll.kernel32.OpenProcess(
            PROCESS_QUERY_LIMITED_INFORMATION, False, pid
        )
        if handle:
            ctypes.windll.kernel32.CloseHandle(handle)
            return True
        return False
    except Exception:
        pass
    # Fallback: tasklist (slow but reliable)
    try:
        out = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True, text=True, timeout=5,
            creationflags=_NO_WINDOW,
        )
        return str(pid) in (out.stdout or "")
    except Exception:
        return False


def _sweep_stale_claims():
    """Conservative PID-based sweeper.

    - Any row in clipper_instances whose pid is not alive → deleted.
    - Any instance stuck in 'starting' or 'waiting_for_account' with a
      heartbeat older than 5 minutes → killed and deleted.
    - Any account whose in_use_pid is not alive → claim cleared.
    - Any processed_pdfs row with claimed_pid not alive and clipped=0 → claim cleared.
    """
    import datetime as _dt
    STALE_STATUSES = ("starting", "waiting_for_account")
    STALE_TIMEOUT = 300  # 5 minutes

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT slot_id, pid, status, heartbeat_at FROM clipper_instances"
        ).fetchall()
        for r in rows:
            killed = False
            if not _pid_alive(r["pid"]):
                print(
                    f"[sweep] removing slot={r['slot_id']} pid={r['pid']} "
                    f"(process not alive)",
                    flush=True,
                )
                killed = True
            elif (r["status"] or "").lower() in STALE_STATUSES and r["heartbeat_at"]:
                try:
                    hb = _dt.datetime.strptime(r["heartbeat_at"], "%Y-%m-%d %H:%M:%S")
                    age = (_dt.datetime.now() - hb).total_seconds()
                    if age > STALE_TIMEOUT:
                        print(
                            f"[sweep] killing slot={r['slot_id']} pid={r['pid']} "
                            f"status={r['status']} stale heartbeat ({age:.0f}s)",
                            flush=True,
                        )
                        try:
                            os.kill(int(r["pid"]), signal.SIGTERM)
                        except Exception:
                            pass
                        killed = True
                except Exception:
                    pass
            if killed:
                conn.execute(
                    "DELETE FROM clipper_instances WHERE slot_id = ?", (r["slot_id"],)
                )
                # Also release any account claimed by this slot
                conn.execute(
                    "UPDATE accounts SET in_use_by = NULL, in_use_since = NULL, in_use_pid = NULL "
                    "WHERE in_use_by = ?", (r["slot_id"],)
                )
                # Release any pages claimed by this slot
                conn.execute(
                    "UPDATE processed_pdfs SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL "
                    "WHERE claimed_by = ?", (r["slot_id"],)
                )

        rows = conn.execute(
            "SELECT email, in_use_pid FROM accounts WHERE in_use_by IS NOT NULL"
        ).fetchall()
        for r in rows:
            if not _pid_alive(r["in_use_pid"]):
                conn.execute(
                    """UPDATE accounts
                          SET in_use_by = NULL, in_use_since = NULL, in_use_pid = NULL
                        WHERE email = ?""",
                    (r["email"],),
                )

        rows = conn.execute(
            "SELECT pdf_filename, claimed_pid FROM processed_pdfs "
            "WHERE claimed_by IS NOT NULL AND (clipped = 0 OR clipped IS NULL)"
        ).fetchall()
        for r in rows:
            if not _pid_alive(r["claimed_pid"]):
                conn.execute(
                    """UPDATE processed_pdfs
                          SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL
                        WHERE pdf_filename = ?""",
                    (r["pdf_filename"],),
                )
        conn.commit()
    finally:
        conn.close()


def _next_free_slot_id():
    """Pick the smallest positive integer slot id not currently in
    clipper_instances. Returned as a string."""
    conn = get_db()
    try:
        taken = set()
        for r in conn.execute("SELECT slot_id FROM clipper_instances").fetchall():
            try:
                taken.add(int(r["slot_id"]))
            except (TypeError, ValueError):
                pass
    finally:
        conn.close()
    i = 1
    while i in taken:
        i += 1
    return str(i)


def _get_state(key, default=""):
    try:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT value FROM clipper_state WHERE key = ?", (key,)
            ).fetchone()
            if row and row["value"] is not None:
                return row["value"]
        finally:
            conn.close()
    except Exception:
        pass
    return default


def _set_state(key, value):
    for _attempt in range(3):
        try:
            conn = get_db()
            try:
                conn.execute(
                    "INSERT OR REPLACE INTO clipper_state (key, value) VALUES (?, ?)",
                    (key, str(value)),
                )
                conn.commit()
                return  # success
            finally:
                conn.close()
        except Exception as e:
            import time as _time
            print(f"[_set_state] attempt {_attempt+1} failed for {key}={value}: {e}", flush=True)
            _time.sleep(1)
    print(f"[_set_state] FAILED to set {key}={value} after 3 attempts", flush=True)


def _log_spawn(msg):
    import datetime as _dt
    print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] [spawn] {msg}", flush=True)


def _spawn_instance(date_start="", date_end="", max_pages="0"):
    """Launch a new detached clip_and_extract.py worker in a fresh slot.
    Returns (slot_id, pid) on success or (None, None) on failure.
    Extracted from /api/instances/add so autoscale can call it too.
    """
    import shutil as _shutil
    try:
        # Abort if global stop is active or target is zero
        if GLOBAL_STOP_FLAG.exists() or STOP_FLAG.exists():
            _log_spawn("spawn aborted — stop flag active")
            return None, None
        try:
            t = int(_get_state("instances_target", "0"))
            if t <= 0:
                _log_spawn("spawn aborted — target is 0")
                return None, None
        except Exception:
            pass
        _sweep_stale_claims()
        slot_id = _next_free_slot_id()
        # Kill any orphan Chrome processes using this slot's profile
        try:
            _target_str = f"chrome_temp_profile_clipper_{slot_id}"
            result = subprocess.run(
                ["wmic", "process", "where", "name='chrome.exe'", "get", "ProcessId,CommandLine"],
                capture_output=True, text=True, timeout=10,
                creationflags=_NO_WINDOW,
            )
            for line in result.stdout.splitlines():
                if _target_str in line:
                    parts = line.strip().split()
                    if parts:
                        try:
                            _pid = int(parts[-1])
                            os.kill(_pid, 9)
                            _log_spawn(f"killed orphan chrome pid={_pid} for slot {slot_id}")
                        except Exception:
                            pass
        except Exception:
            pass
        try:
            _profile_dir = RUNTIME_DIR / f"chrome_temp_profile_clipper_{slot_id}"
            if _profile_dir.exists():
                _shutil.rmtree(_profile_dir, ignore_errors=True)
        except Exception:
            pass
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute(
                "INSERT OR REPLACE INTO clipper_instances "
                "(slot_id, pid, status, started_at, heartbeat_at) "
                "VALUES (?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))",
                (slot_id, 0, "spawning"),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
        # Only clear the per-slot stop flag on spawn. Never clear the global
        # kill switch here — if the user set it, we must not undo that.
        try:
            _slot_flag = _instance_stop_flag(slot_id)
            if _slot_flag.exists():
                if _slot_flag.is_dir():
                    _shutil.rmtree(_slot_flag, ignore_errors=True)
                else:
                    _slot_flag.unlink()
        except Exception:
            pass
        cmd = [
            "python", "clip_and_extract.py",
            f"--slot-id={slot_id}",
            str(max_pages) or "0",
        ]
        if date_start:
            cmd.append(date_start)
            cmd.append(date_end or "2025-12-31")
        _si = subprocess.STARTUPINFO()
        _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        _si.wShowWindow = 0  # SW_HIDE
        import traceback as _tb
        caller = "".join(_tb.format_stack(limit=4)[:-1])
        _log_spawn(
            f"spawning slot={slot_id} date_start={date_start} date_end={date_end}\n"
            f"  called from:\n{caller}"
        )
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            startupinfo=_si,
        )
        # Verify subprocess is alive after 1 second
        import time as _time
        _time.sleep(1)
        if proc.poll() is not None:
            _log_spawn(f"spawned slot={slot_id} but process died immediately (exit={proc.returncode})")
            try:
                conn = sqlite3.connect(str(DB_PATH), timeout=10)
                conn.execute("DELETE FROM clipper_instances WHERE slot_id = ?", (slot_id,))
                conn.commit()
                conn.close()
            except Exception:
                pass
            return None, None
        # Update PID in the row (was 0 from initial insert)
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            conn.execute("UPDATE clipper_instances SET pid = ? WHERE slot_id = ?", (proc.pid, slot_id))
            conn.commit()
            conn.close()
        except Exception:
            pass
        _log_spawn(f"spawned slot={slot_id} pid={proc.pid}")
        return slot_id, proc.pid
    except Exception as e:
        print(f"[autoscale] spawn failed: {e}", flush=True)
        return None, None


import threading as _threading
_autoscale_lock = _threading.Lock()
_last_spawn_time = 0  # epoch timestamp of last spawn
_SPAWN_COOLDOWN = 15  # minimum seconds between spawns


def _autoscale_tick():
    """Read the persisted target and reconcile running instance count.
    Spawns at most one instance per tick to avoid thundering-herd launches.
    """
    global _last_spawn_time
    if not _autoscale_lock.acquire(blocking=False):
        return  # another tick is already running
    try:
        # Enforce minimum cooldown between spawns
        import time as _time
        if _time.time() - _last_spawn_time < _SPAWN_COOLDOWN:
            return
        _sweep_stale_claims()
        target_s = _get_state("instances_target", "0")
        try:
            target = int(target_s)
        except ValueError:
            target = 0
        if target <= 0:
            return
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT status, started_at FROM clipper_instances"
            ).fetchall()
        finally:
            conn.close()
        current = len(rows)
        if current >= target:
            return
        # Don't pile on new browsers while an earlier one is still trying to log
        # in. Block spawning if any instance is in a pre-running state, unless
        # it's been stuck there long enough that it's clearly wedged (then the
        # stale sweep / user will clean it up).
        import datetime as _dt
        now = _dt.datetime.now()
        for r in rows:
            st = (r["status"] or "").lower()
            if st in ("running", "dead", "stopped", "error"):
                continue
            # pending states: spawning, starting, logging-in, etc.
            started = r["started_at"] or ""
            age = 9999
            try:
                t = _dt.datetime.strptime(started, "%Y-%m-%d %H:%M:%S")
                age = (now - t).total_seconds()
            except Exception:
                pass
            if age < 240:  # give each spawn up to 4 minutes to reach running
                print(
                    f"[autoscale] holding off — pending instance status={st} age={int(age)}s"
                )
                return
        # Skip if a global stop flag is set
        if GLOBAL_STOP_FLAG.exists() or STOP_FLAG.exists():
            return
        date_start = _get_state("instances_date_start", "")
        date_end = _get_state("instances_date_end", "")
        # Don't spawn if the configured range has no unclipped pages left —
        # otherwise we'd spin up a new clipper just to have it find an empty
        # queue and exit, forever.
        try:
            q_conn = sqlite3.connect(str(DB_PATH), timeout=30)
            try:
                where = [
                    "(ignored IS NULL OR ignored = 0)",
                    "url IS NOT NULL AND url != ''",
                    "(clipped IS NULL OR clipped = 0)",
                ]
                params = []
                if date_start:
                    where.append("date_str >= ?")
                    params.append(date_start)
                if date_end:
                    where.append("date_str <= ?")
                    params.append(date_end)
                remain = q_conn.execute(
                    "SELECT COUNT(*) FROM processed_pdfs WHERE " + " AND ".join(where),
                    params,
                ).fetchone()[0]
            finally:
                q_conn.close()
        except Exception as _e:
            remain = 1  # fail-open: don't block on a query error
        if remain <= 0:
            # Queue is empty. Check if all instances are done (not actively clipping).
            # If any instance is still running/starting, hold off — it may unclaim pages
            # on failure that need retrying.
            active_statuses = {"running", "starting", "logging-in", "spawning"}
            still_active = [
                r for r in rows
                if (r["status"] or "").lower() in active_statuses
            ]
            if still_active:
                _log_spawn(
                    f"autoscale tick: queue empty but {len(still_active)} instance(s) "
                    f"still active — waiting for them to finish"
                )
                return  # don't spawn more, but don't zero target yet
            # All instances are idle/done/gone — safe to shut down
            _log_spawn(
                f"autoscale tick: queue empty for range {date_start}..{date_end} "
                f"and all instances idle — zeroing target"
            )
            _set_state("instances_target", "0")
            return
        _log_spawn(
            f"autoscale tick: current={current} target={target} remain={remain} — spawning"
        )
        slot_id, pid = _spawn_instance(date_start=date_start, date_end=date_end, max_pages="0")
        if slot_id:
            _last_spawn_time = _time.time()
    except Exception as e:
        print(f"[autoscale] tick error: {e}")
    finally:
        _autoscale_lock.release()


def _autoscale_loop():
    import time as _time
    # Initial delay so the server finishes starting before first tick
    _time.sleep(10)
    while True:
        _autoscale_tick()
        _time.sleep(30)


# === COLLECTOR MULTI-INSTANCE SUPPORT ===

COLLECTOR_GLOBAL_STOP_FLAG = RUNTIME_DIR / "stop_collector_all"


def _collector_instance_stop_flag(slot_id):
    return RUNTIME_DIR / f"stop_collector_{slot_id}"


def _sweep_stale_collector_claims():
    """PID-based sweeper for collector_instances."""
    conn = get_db()
    try:
        # Ensure table exists
        conn.execute("""
            CREATE TABLE IF NOT EXISTS collector_instances (
                slot_id TEXT PRIMARY KEY, pid INTEGER, account_email TEXT,
                status TEXT, current_date TEXT, urls_this_run INTEGER DEFAULT 0,
                date_start TEXT, date_end TEXT, started_at TEXT,
                heartbeat_at TEXT, last_action TEXT
            )
        """)
        conn.commit()
        rows = conn.execute(
            "SELECT slot_id, pid, status FROM collector_instances"
        ).fetchall()
        for r in rows:
            if not _pid_alive(r["pid"]):
                print(
                    f"[collector-sweep] removing slot={r['slot_id']} pid={r['pid']} (dead)",
                    flush=True,
                )
                conn.execute(
                    "DELETE FROM collector_instances WHERE slot_id = ?",
                    (r["slot_id"],),
                )
        conn.commit()
    finally:
        conn.close()


def _next_free_collector_slot_id():
    conn = get_db()
    try:
        taken = set()
        for r in conn.execute("SELECT slot_id FROM collector_instances").fetchall():
            try:
                taken.add(int(r["slot_id"]))
            except (TypeError, ValueError):
                pass
    finally:
        conn.close()
    i = 1
    while i in taken:
        i += 1
    return str(i)


def _list_collector_instances():
    _sweep_stale_collector_claims()
    conn = get_db()
    try:
        # Check if accounts table has urls_today column
        acct_cols = [r[1] for r in conn.execute("PRAGMA table_info(accounts)").fetchall()]
        has_urls_today = "urls_today" in acct_cols and "urls_today_date" in acct_cols
        today_str = datetime.now().strftime("%Y-%m-%d")
        if has_urls_today:
            rows = conn.execute(
                """SELECT ci.slot_id, ci.pid, ci.account_email, ci.status,
                          ci.current_date, ci.urls_this_run, ci.date_start,
                          ci.date_end, ci.started_at, ci.heartbeat_at,
                          ci.last_action,
                          CASE WHEN a.urls_today_date = ? THEN COALESCE(a.urls_today, 0) ELSE 0 END AS urls_today
                     FROM collector_instances ci
                     LEFT JOIN accounts a ON a.email = ci.account_email
                    ORDER BY CAST(ci.slot_id AS INTEGER), ci.slot_id""",
                (today_str,),
            ).fetchall()
        else:
            rows = conn.execute(
                """SELECT slot_id, pid, account_email, status, current_date,
                          urls_this_run, date_start, date_end,
                          started_at, heartbeat_at, last_action
                     FROM collector_instances
                    ORDER BY CAST(slot_id AS INTEGER), slot_id"""
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def _spawn_collector_instance(date_start="", date_end="",
                               max_urls=1500, restart_every=100):
    """Launch a new detached collect_urls.py worker in a fresh slot."""
    import shutil as _shutil
    try:
        if COLLECTOR_GLOBAL_STOP_FLAG.exists():
            return None, None
        try:
            t = int(_get_state("collector_instances_target", "0"))
            if t <= 0:
                return None, None
        except Exception:
            pass
        _sweep_stale_collector_claims()
        slot_id = _next_free_collector_slot_id()
        # Clean profile
        try:
            _profile_dir = RUNTIME_DIR / f"chrome_temp_profile_collector_{slot_id}"
            if _profile_dir.exists():
                _shutil.rmtree(_profile_dir, ignore_errors=True)
        except Exception:
            pass
        # Pre-create instance row
        try:
            conn = sqlite3.connect(str(DB_PATH))
            conn.execute(
                "INSERT OR REPLACE INTO collector_instances "
                "(slot_id, pid, status, started_at, heartbeat_at) "
                "VALUES (?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))",
                (slot_id, 0, "spawning"),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass
        # Clear per-slot stop flag
        try:
            _slot_flag = _collector_instance_stop_flag(slot_id)
            if _slot_flag.exists():
                if _slot_flag.is_dir():
                    _shutil.rmtree(_slot_flag, ignore_errors=True)
                else:
                    _slot_flag.unlink()
        except Exception:
            pass
        cmd = [
            "python", "collect_urls.py",
            f"--slot-id={slot_id}",
            "--start", date_start,
            "--end", date_end,
            "--restart-every", str(restart_every),
            "--max-urls", str(max_urls),
        ]
        _si = subprocess.STARTUPINFO()
        _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
        _si.wShowWindow = 0
        print(f"[collector-spawn] spawning slot={slot_id}", flush=True)
        proc = subprocess.Popen(
            cmd,
            cwd=str(BASE_DIR),
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
            startupinfo=_si,
        )
        import time as _time
        _time.sleep(1)
        if proc.poll() is not None:
            print(f"[collector-spawn] slot={slot_id} died immediately", flush=True)
            try:
                conn = sqlite3.connect(str(DB_PATH), timeout=10)
                conn.execute("DELETE FROM collector_instances WHERE slot_id = ?",
                             (slot_id,))
                conn.commit()
                conn.close()
            except Exception:
                pass
            return None, None
        # Update PID
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            conn.execute("UPDATE collector_instances SET pid = ? WHERE slot_id = ?",
                         (proc.pid, slot_id))
            conn.commit()
            conn.close()
        except Exception:
            pass
        print(f"[collector-spawn] spawned slot={slot_id} pid={proc.pid}", flush=True)
        return slot_id, proc.pid
    except Exception as e:
        print(f"[collector-spawn] failed: {e}", flush=True)
        return None, None


_collector_autoscale_lock = _threading.Lock()
_last_collector_spawn_time = 0


def _collector_autoscale_tick():
    global _last_collector_spawn_time
    if not _collector_autoscale_lock.acquire(blocking=False):
        return
    try:
        import time as _time
        if _time.time() - _last_collector_spawn_time < _SPAWN_COOLDOWN:
            return
        _sweep_stale_collector_claims()
        try:
            target = int(_get_state("collector_instances_target", "0"))
        except ValueError:
            target = 0
        if target <= 0:
            return
        conn = get_db()
        try:
            rows = conn.execute(
                "SELECT status, started_at FROM collector_instances"
            ).fetchall()
        finally:
            conn.close()
        current = len(rows)
        if current >= target:
            return
        # Don't spawn while another is still starting
        import datetime as _dt
        now = _dt.datetime.now()
        for r in rows:
            st = (r["status"] or "").lower()
            if st in ("running", "stopped"):
                continue
            started = r["started_at"] or ""
            age = 9999
            try:
                t = _dt.datetime.strptime(started, "%Y-%m-%d %H:%M:%S")
                age = (now - t).total_seconds()
            except Exception:
                pass
            if age < 240:
                return
        if COLLECTOR_GLOBAL_STOP_FLAG.exists():
            return
        date_start = _get_state("collector_instances_date_start", "")
        date_end = _get_state("collector_instances_date_end", "")
        max_urls = int(_get_state("collector_instances_max_urls", "1500") or 1500)
        restart_every = int(_get_state("collector_instances_restart_every", "100") or 100)
        slot_id, pid = _spawn_collector_instance(
            date_start=date_start, date_end=date_end,
            max_urls=max_urls, restart_every=restart_every,
        )
        if slot_id:
            _last_collector_spawn_time = _time.time()
    except Exception as e:
        print(f"[collector-autoscale] tick error: {e}", flush=True)
    finally:
        _collector_autoscale_lock.release()


def _collector_autoscale_loop():
    import time as _time
    _time.sleep(15)
    while True:
        _collector_autoscale_tick()
        _time.sleep(30)


def _list_instances():
    """Return current clipper_instances rows as plain dicts (after sweep).
    Also joins the account's persistent clips_this_session and the daily
    limit so the UI can show cumulative usage vs cap."""
    _sweep_stale_claims()
    conn = get_db()
    try:
        rows = conn.execute(
            """SELECT i.slot_id, i.pid, i.account_email, i.status, i.current_date,
                      i.current_page, i.count_this_run, i.date_start, i.date_end,
                      i.started_at, i.heartbeat_at, i.last_action, i.browser_health,
                      CASE WHEN a.clips_today_date = date('now','localtime')
                           THEN COALESCE(a.clips_today, 0) ELSE 0 END AS account_clips_today
                 FROM clipper_instances i
                 LEFT JOIN accounts a ON a.email = i.account_email
                ORDER BY CAST(i.slot_id AS INTEGER), i.slot_id"""
        ).fetchall()
        result = [dict(r) for r in rows]
        try:
            lim_row = conn.execute(
                "SELECT value FROM clipper_state WHERE key='daily_clip_limit'"
            ).fetchone()
            daily_limit = int(lim_row["value"]) if lim_row and lim_row["value"] else 0
        except Exception:
            daily_limit = 0
        for r in result:
            r["daily_clip_limit"] = daily_limit
        return result
    finally:
        conn.close()


def ensure_accounts_table():
    conn = get_db()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            password TEXT NOT NULL,
            label TEXT DEFAULT '',
            subscription_type TEXT DEFAULT '',
            active INTEGER DEFAULT 1,
            total_clips INTEGER DEFAULT 0,
            clips_this_session INTEGER DEFAULT 0,
            last_clip_time TEXT,
            last_throttle_time TEXT,
            throttle_count INTEGER DEFAULT 0,
            avg_clips_before_throttle REAL DEFAULT 0,
            last_login_time TEXT,
            last_logout_time TEXT,
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT (datetime('now','localtime')),
            updated_at TEXT DEFAULT (datetime('now','localtime'))
        )
    """)
    conn.commit()
    conn.close()


ensure_accounts_table()


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
        "SELECT a.*, pp.url AS page_url, pp.clip_url, pp.thumbnail_path FROM articles a LEFT JOIN processed_pdfs pp ON a.pdf_filename = pp.pdf_filename ORDER BY a.date, a.page"
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

    # No-articles count (for stats)
    no_articles_count = conn.execute("""
        SELECT COUNT(*) as c FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        AND (pp.ignored IS NULL OR pp.ignored = 0)
    """).fetchone()["c"]

    # No-articles filenames for aggregation
    na_fnames = conn.execute("""
        SELECT pp.pdf_filename FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        AND (pp.ignored IS NULL OR pp.ignored = 0)
    """).fetchall()

    # Parse dates from filenames for monthly/yearly counts
    na_monthly = {}
    na_yearly = {}
    for row in na_fnames:
        m = re.search(r'_(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', row["pdf_filename"])
        if m:
            ym = f"{m.group(1)}-{m.group(2)}"
            y = m.group(1)
            na_monthly[ym] = na_monthly.get(ym, 0) + 1
            na_yearly[y] = na_yearly.get(y, 0) + 1

    # Monthly reference counts
    monthly_rows = conn.execute("""
        SELECT substr(date, 1, 7) as ym, COUNT(*) as c FROM articles
        WHERE date IS NOT NULL AND length(date) >= 7
        GROUP BY ym
    """).fetchall()
    monthly = {r["ym"]: r["c"] for r in monthly_rows}
    for ym, c in na_monthly.items():
        monthly[ym] = monthly.get(ym, 0) + c
    monthly_sorted = sorted(monthly.items())

    # Articles by year
    articles_by_year_rows = conn.execute("""
        SELECT substr(date, 1, 4) as y, COUNT(*) as c FROM articles
        WHERE date IS NOT NULL AND length(date) >= 4
        GROUP BY y ORDER BY y
    """).fetchall()
    articles_by_year_sorted = [(r["y"], r["c"]) for r in articles_by_year_rows]

    # No articles by year
    no_articles_by_year_sorted = sorted(na_yearly.items())

    # Ignored by year (pages with ignored=1, no articles)
    ignored_fnames = conn.execute("""
        SELECT pp.pdf_filename FROM processed_pdfs pp
        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
        WHERE a.id IS NULL AND pp.articles_found != -1
        AND pp.ignored = 1
    """).fetchall()
    ignored_yearly = {}
    for row in ignored_fnames:
        m = re.search(r'_(\d{4})_(\d{2})_(\d{2})_\d+\.pdf$', row["pdf_filename"])
        if m:
            y = m.group(1)
            ignored_yearly[y] = ignored_yearly.get(y, 0) + 1
    ignored_by_year_sorted = sorted(ignored_yearly.items())

    conn.close()

    return {
        "stats": stats,
        "articles": article_list,
        "quotes": quote_list,
        "people": people_list,
        "log": log_list,
        "no_articles_count": no_articles_count,
        "monthly": monthly_sorted,
        "articles_by_year": articles_by_year_sorted,
        "no_articles_by_year": no_articles_by_year_sorted,
        "ignored_by_year": ignored_by_year_sorted,
    }


def get_no_articles_page(page=1, per_page=100, sort="desc", filter_text="", filter_mode="keep"):
    """Return one page of no-articles entries, sorted by date.
    filter_mode: 'keep' (non-ignored only), 'ignored' (ignored only), 'all' (both)
    """
    conn = get_db()
    order = "DESC" if sort == "desc" else "ASC"
    offset = (page - 1) * per_page

    if filter_mode == "ignored":
        ignore_clause = "AND pp.ignored = 1"
    elif filter_mode == "all":
        ignore_clause = ""
    else:
        ignore_clause = "AND (pp.ignored IS NULL OR pp.ignored = 0)"

    # Count total (with optional filter)
    if filter_text:
        like = f"%{filter_text}%"
        total = conn.execute(f"""
            SELECT COUNT(*) as c FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            {ignore_clause}
            AND pp.pdf_filename LIKE ?
        """, (like,)).fetchone()["c"]
        rows = conn.execute(f"""
            SELECT pp.pdf_filename, pp.search_term, pp.url, pp.clip_url,
                   pp.thumbnail_path, pp.ignored, pp.highlighted, pp.has_photo,
                   pp.auto_ignore_confidence
            FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            {ignore_clause}
            AND pp.pdf_filename LIKE ?
            ORDER BY pp.date_str {order}, pp.pdf_filename {order}
            LIMIT ? OFFSET ?
        """, (like, per_page, offset)).fetchall()
    else:
        total = conn.execute(f"""
            SELECT COUNT(*) as c FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            {ignore_clause}
        """).fetchone()["c"]
        rows = conn.execute(f"""
            SELECT pp.pdf_filename, pp.search_term, pp.url, pp.clip_url,
                   pp.thumbnail_path, pp.ignored, pp.highlighted, pp.has_photo,
                   pp.auto_ignore_confidence
            FROM processed_pdfs pp
            LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
            WHERE a.id IS NULL AND pp.articles_found != -1
            {ignore_clause}
            ORDER BY pp.date_str {order}, pp.pdf_filename {order}
            LIMIT ? OFFSET ?
        """, (per_page, offset)).fetchall()

    items = []
    for row in rows:
        fname = row["pdf_filename"]
        m = re.match(
            r"^(?P<paper>.+)_(?P<year>\d{4})_(?P<month>\d{2})_(?P<day>\d{2})_(?P<page>\d+)\.pdf$",
            fname, re.IGNORECASE
        )
        if m:
            newspaper = m.group("paper").replace("_", " ")
            date_str = f"{m.group('year')}-{m.group('month')}-{m.group('day')}"
            pg = int(m.group("page"))
        else:
            newspaper = fname
            date_str = "?"
            pg = 0

        items.append({
            "filename": fname,
            "date": date_str,
            "page": pg,
            "newspaper": newspaper,
            "search_term": row["search_term"],
            "url": row["url"] or "",
            "clip_url": row["clip_url"] or "",
            "thumbnail": row["thumbnail_path"] or "",
            "ignored": row["ignored"] or 0,
            "highlighted": row["highlighted"] or 0,
            "has_photo": row["has_photo"] or 0,
            "confidence": row["auto_ignore_confidence"],
        })

    conn.close()
    total_pages = (total + per_page - 1) // per_page
    return {
        "items": items,
        "total": total,
        "page": page,
        "per_page": per_page,
        "total_pages": total_pages,
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
    """Disabled — was launching stray Chrome browsers via undetected_chromedriver."""
    raise RuntimeError("Chrome fetch disabled — use direct HTTP instead")


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
        # Inherit page-level flags set by user on the "no articles" tab (promote only)
        pdf_row = conn.execute(
            "SELECT highlighted, has_photo FROM processed_pdfs WHERE pdf_filename = ?",
            (pdf_filename,)
        ).fetchone() if pdf_filename else None
        pdf_highlighted = (pdf_row["highlighted"] or 0) if pdf_row else 0
        pdf_has_photo = (pdf_row["has_photo"] or 0) if pdf_row else 0
        conn.execute(
            """UPDATE articles SET full_text = ?, headline = ?, has_image = ?,
                                   clip_id = ?, newspaper = ?,
                                   highlighted = COALESCE(NULLIF(highlighted,0), ?),
                                   has_photo   = COALESCE(NULLIF(has_photo,0), ?)
               WHERE id = ?""",
            (ocr_text, use_headline, has_image, clip_id or None, newspaper, pdf_highlighted, pdf_has_photo, match_id),
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
        # Inherit page-level flags set by user on the "no articles" tab
        pdf_row = conn.execute(
            "SELECT highlighted, has_photo FROM processed_pdfs WHERE pdf_filename = ?",
            (pdf_filename,)
        ).fetchone() if pdf_filename else None
        pdf_highlighted = (pdf_row["highlighted"] or 0) if pdf_row else 0
        pdf_has_photo = (pdf_row["has_photo"] or 0) if pdf_row else 0
        cur = conn.execute(
            """INSERT INTO articles (date, newspaper, page, headline, full_text,
                                     pdf_filename, has_image, search_term, clip_id, highlighted, has_photo)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (date, newspaper, page, headline, ocr_text, pdf_filename, has_image, search_term, clip_id or None, pdf_highlighted, pdf_has_photo),
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

    def log_message(self, fmt, *args):
        # Route access log to stdout so it gets captured alongside other
        # diagnostics. Quiet noisy static/asset requests.
        try:
            msg = fmt % args
        except Exception:
            msg = str(args)
        path = getattr(self, "path", "")
        if any(p in path for p in ("/api/instances/add", "/api/instances/stop",
                                    "/api/instances/set_target",
                                    "/api/instances/clear_stop_all")):
            import datetime as _dt
            print(f"[{_dt.datetime.now().strftime('%H:%M:%S')}] [http] {msg}", flush=True)

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
        elif self.path == "/api/no-articles/count":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                try:
                    row = conn.execute(
                        "SELECT COUNT(*) as c FROM processed_pdfs "
                        "WHERE articles_found = 0 OR articles_found IS NULL"
                    ).fetchone()
                finally:
                    conn.close()
                self.wfile.write(json.dumps({"count": row["c"]}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/no-articles"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                from urllib.parse import urlparse, parse_qs
                params = parse_qs(urlparse(self.path).query)
                page = int(params.get("page", [1])[0])
                per_page = int(params.get("per_page", [100])[0])
                sort = params.get("sort", ["desc"])[0]
                filter_text = params.get("filter", [""])[0]
                filter_mode = params.get("filter_mode", ["keep"])[0]
                data = get_no_articles_page(page, per_page, sort, filter_text, filter_mode=filter_mode)
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
        elif self.path == "/api/screenshots":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import glob as g
                screenshots = sorted(
                    g.glob(str(LOG_DIR / "screenshot_*.png")),
                    key=os.path.getmtime, reverse=True
                )
                files = []
                for path in screenshots:
                    name = os.path.basename(path)
                    mtime = datetime.fromtimestamp(os.path.getmtime(path)).strftime("%Y-%m-%d %H:%M:%S")
                    files.append({"name": name, "time": mtime})
                self.wfile.write(json.dumps(files).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/screenshot/"):
            filename = self.path.split("/api/screenshot/")[1]
            filepath = LOG_DIR / filename
            if filepath.exists() and filepath.suffix == ".png" and ".." not in filename:
                self.send_response(200)
                self.send_header("Content-type", "image/png")
                self.end_headers()
                with open(filepath, "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_response(404)
                self.end_headers()
        elif self.path == "/api/accounts":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                rows = conn.execute("""
                    SELECT *,
                           CASE WHEN clips_today_date = date('now','localtime')
                                THEN COALESCE(clips_today, 0) ELSE 0 END AS clips_today_actual
                    FROM accounts ORDER BY active DESC, total_clips DESC
                """).fetchall()
                accounts = [dict(r) for r in rows]
                # Include daily clip limit so the UI can determine cooldown status
                limit_row = conn.execute("SELECT value FROM clipper_state WHERE key='daily_clip_limit'").fetchone()
                clip_limit = int(limit_row[0]) if limit_row and limit_row[0] else 250
                conn.close()
                self.wfile.write(json.dumps({"accounts": accounts, "daily_clip_limit": clip_limit}, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/clipper/range_stats"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(self.path).query)
                ds = (q.get("start", [""])[0] or "").strip() or None
                de = (q.get("end", [""])[0] or "").strip() or None
                rs_conn = sqlite3.connect(str(DB_PATH), timeout=30)
                rs_conn.row_factory = sqlite3.Row
                try:
                    where = [
                        "(ignored IS NULL OR ignored = 0)",
                        "url IS NOT NULL",
                        "url != ''",
                    ]
                    params = []
                    if ds:
                        where.append("date_str >= ?")
                        params.append(ds)
                    if de:
                        where.append("date_str <= ?")
                        params.append(de)
                    where_sql = " AND ".join(where)
                    total = rs_conn.execute(
                        f"SELECT COUNT(*) AS c FROM processed_pdfs WHERE {where_sql}",
                        params,
                    ).fetchone()["c"]
                    done = rs_conn.execute(
                        f"SELECT COUNT(*) AS c FROM processed_pdfs WHERE {where_sql} AND clipped = 1",
                        params,
                    ).fetchone()["c"]
                finally:
                    rs_conn.close()
                remain = max(total - done, 0)
                self.wfile.write(json.dumps({
                    "start": ds,
                    "end": de,
                    "total": total,
                    "done": done,
                    "remain": remain,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/daily_stats":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import time as _t
                _now_mono = _t.monotonic()
                if (_daily_stats_cache["out"] is not None
                        and (_now_mono - _daily_stats_cache["ts"]) < _DAILY_STATS_TTL):
                    self.wfile.write(json.dumps(_daily_stats_cache["out"]).encode())
                    return
                now = datetime.now()
                midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
                run_set_start = _determine_run_set_start()

                mid_logs = _scan_logs_since(midnight)
                rs_logs = _scan_logs_since(run_set_start) if run_set_start else {"clipped": 0, "errors": 0}

                stats_conn = sqlite3.connect(str(DB_PATH), timeout=30)
                stats_conn.row_factory = sqlite3.Row
                try:
                    mid_art, mid_pic = _count_articles_since(stats_conn, midnight)
                    rs_art, rs_pic = _count_articles_since(stats_conn, run_set_start)
                    queue = _current_queue_size(stats_conn)
                finally:
                    stats_conn.close()

                out = {
                    "since_midnight": {
                        "clipped": mid_logs["clipped"],
                        "articles": mid_art,
                        "picture_heavy": mid_pic,
                        "errors": mid_logs["errors"],
                        "queue": queue,
                        "start": midnight.strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    "run_set": {
                        "clipped": rs_logs["clipped"],
                        "articles": rs_art,
                        "picture_heavy": rs_pic,
                        "errors": rs_logs["errors"],
                        "queue": queue,
                        "start": run_set_start.strftime("%Y-%m-%d %H:%M:%S") if run_set_start else None,
                    },
                    "running": _is_clipper_running(),
                }
                _daily_stats_cache["out"] = out
                _daily_stats_cache["ts"] = _now_mono
                self.wfile.write(json.dumps(out).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/book_notes":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                text = BOOK_NOTES_PATH.read_text(encoding="utf-8") if BOOK_NOTES_PATH.exists() else ""
            except Exception:
                text = ""
            self.wfile.write(json.dumps({"text": text}).encode())
        elif self.path == "/api/reextract/status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                status_file = RUNTIME_DIR / "reextract_status.json"
                if status_file.exists():
                    data = json.loads(status_file.read_text(encoding="utf-8"))
                else:
                    data = {"status": "idle"}
                # Also count how many pages are eligible
                conn = get_db()
                eligible = conn.execute("""
                    SELECT COUNT(*) as c FROM processed_pdfs
                    WHERE clipped = 1 AND articles_found = 0
                      AND ocr_text IS NOT NULL AND length(ocr_text) > 100
                      AND lower(ocr_text) LIKE '%lake%worth%'
                """).fetchone()["c"]
                conn.close()
                data["eligible"] = eligible
                self.wfile.write(json.dumps(data).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"status": "idle", "error": str(e)}).encode())
        elif self.path == "/api/clipper/progress":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                log_files = sorted(glob.glob(str(LOG_DIR / "clipper_*.log")), key=os.path.getmtime, reverse=True)
                progress = {"clipped": 0, "articles": 0, "errors": 0, "queue": 0,
                            "current_page": "", "account": "", "running": _is_clipper_running(),
                            "last_update": "", "page_time": "", "session_clips": 0, "clip_limit": 0}
                if log_files:
                    with open(log_files[0], "r", encoding="utf-8", errors="replace") as f:
                        lines = f.readlines()
                    for line in lines:
                        if "Queue:" in line:
                            m = re.search(r'Queue:\s*(\d+)', line)
                            if m:
                                progress["queue"] = int(m.group(1))
                        if "Progress:" in line:
                            m = re.search(r'(\d+) clipped, (\d+) articles?, (\d+) errors?', line)
                            if m:
                                progress["clipped"] = int(m.group(1))
                                progress["articles"] = int(m.group(2))
                                progress["errors"] = int(m.group(3))
                            # Extract timestamp
                            tm = re.match(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', line)
                            if tm:
                                progress["last_update"] = tm.group(1)
                        if "Tracking as account:" in line:
                            m = re.search(r'Tracking as account:\s*(\S+)', line)
                            if m:
                                progress["account"] = m.group(1)
                        if "Rotated to" in line or "Switched to" in line:
                            m = re.search(r'(?:Rotated|Switched) to\s*(\S+@\S+\.com)', line)
                            if m:
                                progress["account"] = m.group(1)
                        if re.search(r'\[\d+/\d+\]', line):
                            m = re.search(r'\[\d+/\d+\]\s*(.*)', line)
                            if m:
                                progress["current_page"] = m.group(1).strip()
                        if "DONE" in line:
                            progress["running"] = False
                        # Extract per-page time
                        tm2 = re.search(r'Page:.*?(\d+\.\d+)s', line)
                        if tm2:
                            progress["page_time"] = tm2.group(1) + "s"
                # When not running, show next eligible account from DB (log account is stale)
                # When running, use the account from the log (most recent Tracking/Switched/Rotated)
                try:
                    acc_conn = sqlite3.connect(str(BASE_DIR / "lake_worth.db"))
                    acc_conn.row_factory = sqlite3.Row
                    acct_email = None
                    if not progress["running"]:
                        next_row = acc_conn.execute(
                            "SELECT email, clips_this_session FROM accounts WHERE active = 1"
                            " AND (last_throttle_time IS NULL OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))"
                            " ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC LIMIT 1"
                        ).fetchone()
                        if next_row:
                            progress["account"] = next_row["email"]
                            progress["session_clips"] = next_row["clips_this_session"] or 0
                            acct_email = next_row["email"]
                    else:
                        acct_email = progress["account"]
                        if acct_email:
                            acc_row = acc_conn.execute(
                                "SELECT clips_this_session FROM accounts WHERE email = ?",
                                (acct_email,)
                            ).fetchone()
                            if acc_row:
                                progress["session_clips"] = acc_row["clips_this_session"] or 0
                    # Count articles from pages clipped by this account (parsed from logs)
                    if acct_email and log_files:
                        acct_pages = []
                        is_acct = False
                        for lf in log_files:
                            try:
                                with open(lf, "r", encoding="utf-8", errors="replace") as lfile:
                                    for ln in lfile:
                                        if "Tracking as account:" in ln:
                                            is_acct = acct_email in ln
                                        elif "Rotated to" in ln or "Switched to" in ln:
                                            is_acct = acct_email in ln
                                        if is_acct and "Page:" in ln:
                                            pm = re.search(r'Page:\s*(\S+\.pdf)', ln)
                                            if pm:
                                                acct_pages.append(pm.group(1))
                            except Exception:
                                pass
                        if acct_pages:
                            placeholders = ",".join(["?"] * len(acct_pages))
                            art_count = acc_conn.execute(
                                f"SELECT COUNT(*) as c FROM articles WHERE pdf_filename IN ({placeholders})",
                                acct_pages
                            ).fetchone()
                            progress["articles"] = art_count["c"] if art_count else 0
                    acc_conn.close()
                except Exception:
                    pass
                # Look up clip limit and next eligible account from DB
                try:
                    state_conn = sqlite3.connect(str(BASE_DIR / "lake_worth.db"))
                    state_conn.row_factory = sqlite3.Row
                    lim_row = state_conn.execute(
                        "SELECT value FROM clipper_state WHERE key = 'daily_clip_limit'"
                    ).fetchone()
                    if lim_row:
                        progress["clip_limit"] = int(lim_row["value"] or 0)
                    # Find next eligible account (not throttled within 24h)
                    next_row = state_conn.execute(
                        "SELECT email, clips_this_session FROM accounts WHERE active = 1"
                        " AND (last_throttle_time IS NULL OR last_throttle_time < datetime('now', 'localtime', '-24 hours'))"
                        " ORDER BY last_throttle_time ASC NULLS FIRST, total_clips ASC LIMIT 1"
                    ).fetchone()
                    if next_row:
                        progress["next_account"] = next_row["email"]
                        progress["next_account_clips"] = next_row["clips_this_session"] or 0
                    state_conn.close()
                except Exception:
                    pass
                self.wfile.write(json.dumps(progress).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/log":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                log_files = sorted(glob.glob(str(LOG_DIR / "clipper_*.log")), key=os.path.getmtime, reverse=True)
                if log_files:
                    with open(log_files[0], "r", encoding="utf-8", errors="replace") as f:
                        all_lines = f.readlines()
                        tail = all_lines[-500:]
                        lines = "".join(reversed(tail))
                else:
                    lines = "No clipper log files found."
                self.wfile.write(json.dumps({"lines": lines}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/settings":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("CREATE TABLE IF NOT EXISTS clipper_state (key TEXT PRIMARY KEY, value TEXT)")
                conn.commit()
                settings = {}
                for row in conn.execute("SELECT key, value FROM clipper_state").fetchall():
                    settings[row["key"]] = row["value"]
                conn.close()
                # Add live status
                settings["stop_flag_active"] = STOP_FLAG.exists()
                settings["clipper_running"] = _is_clipper_running()
                self.wfile.write(json.dumps(settings, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                instances = _list_instances()
                self.wfile.write(json.dumps({
                    "instances": instances,
                    "global_stop": GLOBAL_STOP_FLAG.exists(),
                }, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                instances = _list_collector_instances()
                target = int(_get_state("collector_instances_target", "0") or 0)
                self.wfile.write(json.dumps({
                    "instances": instances,
                    "target": target,
                    "global_stop": COLLECTOR_GLOBAL_STOP_FLAG.exists(),
                }, default=str).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/collector/range_stats"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                from urllib.parse import urlparse, parse_qs
                q = parse_qs(urlparse(self.path).query)
                ds = (q.get("start", [""])[0] or "").strip() or None
                de = (q.get("end", [""])[0] or "").strip() or None
                rs_conn = sqlite3.connect(str(DB_PATH), timeout=30)
                rs_conn.row_factory = sqlite3.Row
                try:
                    where = []
                    params = []
                    if ds:
                        where.append("date_str >= ?")
                        params.append(ds)
                    if de:
                        where.append("date_str <= ?")
                        params.append(de)
                    where_sql = " AND ".join(where) if where else "1=1"
                    total = rs_conn.execute(
                        f"SELECT COUNT(*) AS c FROM processed_pdfs WHERE {where_sql}",
                        params,
                    ).fetchone()["c"]
                    done = rs_conn.execute(
                        f"SELECT COUNT(*) AS c FROM processed_pdfs WHERE {where_sql} AND clipped = 1",
                        params,
                    ).fetchone()["c"]
                    total_all = rs_conn.execute(
                        "SELECT COUNT(*) AS c FROM processed_pdfs"
                    ).fetchone()["c"]
                finally:
                    rs_conn.close()
                remain = max(total - done, 0)
                self.wfile.write(json.dumps({
                    "start": ds,
                    "end": de,
                    "total": total,
                    "done": done,
                    "remain": remain,
                    "total_all": total_all,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/account_stats":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                try:
                    # Per-account URL gathering counts
                    # Check if daily tracking columns exist in accounts table
                    has_daily_cols = False
                    try:
                        col_info = conn.execute("PRAGMA table_info(accounts)").fetchall()
                        col_names = {c["name"] for c in col_info}
                        has_daily_cols = "urls_today" in col_names
                    except Exception:
                        pass
                    if has_daily_cols:
                        rows = conn.execute("""
                            SELECT a.email, a.active, a.in_use_by, a.in_use_pid,
                                   a.total_clips, a.last_clip_time,
                                   COALESCE(g.gathered, 0) AS urls_gathered,
                                   g.last_gathered_time AS join_last_gathered_time,
                                   a.urls_today, a.urls_today_date,
                                   a.last_gathered_time AS acct_last_gathered_time,
                                   a.total_urls_gathered
                            FROM accounts a
                            LEFT JOIN (
                                SELECT gathered_by, COUNT(*) AS gathered,
                                       MAX(processed_at) AS last_gathered_time
                                FROM processed_pdfs
                                WHERE gathered_by IS NOT NULL AND gathered_by != ''
                                GROUP BY gathered_by
                            ) g ON g.gathered_by = a.email
                            WHERE a.active = 1
                            ORDER BY a.email
                        """).fetchall()
                    else:
                        rows = conn.execute("""
                            SELECT a.email, a.active, a.in_use_by, a.in_use_pid,
                                   a.total_clips, a.last_clip_time,
                                   COALESCE(g.gathered, 0) AS urls_gathered,
                                   g.last_gathered_time
                            FROM accounts a
                            LEFT JOIN (
                                SELECT gathered_by, COUNT(*) AS gathered,
                                       MAX(processed_at) AS last_gathered_time
                                FROM processed_pdfs
                                WHERE gathered_by IS NOT NULL AND gathered_by != ''
                                GROUP BY gathered_by
                            ) g ON g.gathered_by = a.email
                            WHERE a.active = 1
                            ORDER BY a.email
                        """).fetchall()
                    # Get actively collecting accounts from collector_instances
                    collecting_emails = set()
                    try:
                        ci_rows = conn.execute(
                            "SELECT account_email FROM collector_instances "
                            "WHERE status IN ('running', 'logging-in', 'starting')"
                        ).fetchall()
                        for ci in ci_rows:
                            if ci["account_email"]:
                                collecting_emails.add(ci["account_email"])
                    except Exception:
                        pass
                finally:
                    conn.close()
                accounts = []
                for r in rows:
                    acct = {
                        "email": r["email"],
                        "active": r["active"],
                        "in_use_by": r["in_use_by"],
                        "in_use_pid": r["in_use_pid"],
                        "total_clips": r["total_clips"],
                        "last_clip_time": r["last_clip_time"],
                        "urls_gathered": r["urls_gathered"],
                        "is_collecting": r["email"] in collecting_emails,
                    }
                    if has_daily_cols:
                        acct["urls_today"] = r["urls_today"] or 0
                        acct["urls_today_date"] = r["urls_today_date"]
                        acct["last_gathered_time"] = r["acct_last_gathered_time"] or r["join_last_gathered_time"]
                        acct["total_urls_gathered"] = r["total_urls_gathered"] or 0
                    else:
                        acct["last_gathered_time"] = r["last_gathered_time"]
                    accounts.append(acct)
                self.wfile.write(json.dumps({
                    "accounts": accounts,
                    "collecting_emails": list(collecting_emails),
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/log":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                coll_log_dir = BASE_DIR / "collector_logs"
                log_files = sorted(glob.glob(str(coll_log_dir / "collector_*.log")), key=os.path.getmtime, reverse=True)
                if log_files:
                    with open(log_files[0], "r", encoding="utf-8", errors="replace") as f:
                        all_lines = f.readlines()
                        tail = all_lines[-500:]
                        lines = "".join(reversed(tail))
                else:
                    lines = "No collector log files found."
                self.wfile.write(json.dumps({"lines": lines}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/last-noarticle-date":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                try:
                    row = conn.execute("""
                        SELECT MAX(pp.date_str) as max_date
                        FROM processed_pdfs pp
                        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
                        WHERE a.id IS NULL AND pp.articles_found != -1
                          AND (pp.ignored IS NULL OR pp.ignored = 0)
                          AND pp.date_str IS NOT NULL AND pp.date_str != '?'
                    """).fetchone()
                finally:
                    conn.close()
                self.wfile.write(json.dumps({"date": row["max_date"] if row else None}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/auto-ignore/last-ignored-date":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                try:
                    row = conn.execute("""
                        SELECT MAX(pp.date_str) as max_date
                        FROM processed_pdfs pp
                        LEFT JOIN articles a ON a.pdf_filename = pp.pdf_filename
                        WHERE a.id IS NULL AND pp.articles_found != -1
                          AND pp.ignored = 1
                          AND pp.date_str IS NOT NULL AND pp.date_str != '?'
                    """).fetchone()
                finally:
                    conn.close()
                self.wfile.write(json.dumps({"date": row["max_date"] if row else None}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                try:
                    rows = conn.execute(
                        "SELECT key, value FROM clipper_state WHERE key LIKE 'collector_%'"
                    ).fetchall()
                finally:
                    conn.close()
                status = {}
                for r in rows:
                    k = r["key"].replace("collector_", "", 1)
                    status[k] = r["value"]
                # Check if PID is still alive — clear stale state in DB
                pid = int(status.get("pid", 0) or 0)
                if pid and not _pid_alive(pid):
                    status["status"] = "stopped"
                    status["pid"] = ""
                    try:
                        conn2 = get_db()
                        conn2.execute("UPDATE clipper_state SET value = 'stopped' WHERE key = 'collector_status' AND value = 'running'")
                        conn2.execute("UPDATE clipper_state SET value = '' WHERE key = 'collector_pid'")
                        conn2.commit()
                        conn2.close()
                    except Exception:
                        pass
                # Add actual URL count from DB for the active date range
                ds = status.get("date_start", "")
                de = status.get("date_end", "")
                if ds and de:
                    try:
                        cnt_conn = get_db()
                        try:
                            status["urls_in_range"] = cnt_conn.execute(
                                "SELECT COUNT(*) FROM processed_pdfs "
                                "WHERE date_str >= ? AND date_str <= ?",
                                (ds, de),
                            ).fetchone()[0]
                        finally:
                            cnt_conn.close()
                    except Exception:
                        pass
                self.wfile.write(json.dumps(status).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path.startswith("/api/auto-ignore/count"):
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                qs = self.path.split("?", 1)[1] if "?" in self.path else ""
                params = dict(p.split("=", 1) for p in qs.split("&") if "=" in p)
                ds = params.get("date_start", "1900-01-01")
                de = params.get("date_end", "2000-12-31")
                conn = get_db()
                try:
                    row = conn.execute("""
                        SELECT COUNT(*) as c FROM processed_pdfs
                        WHERE date_str >= ? AND date_str <= ?
                          AND (clipped = 0 OR clipped IS NULL)
                          AND (ignored IS NULL OR ignored = 0)
                          AND thumbnail_path IS NOT NULL
                    """, (ds, de)).fetchone()
                finally:
                    conn.close()
                self.wfile.write(json.dumps({"count": row["c"]}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/auto-ignore/status":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _ai_status_file = RUNTIME_DIR / "auto_ignore_status.json"
                if _ai_status_file.exists():
                    data = json.loads(_ai_status_file.read_text(encoding="utf-8"))
                else:
                    data = {"status": "idle"}
                self.wfile.write(json.dumps(data).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"status": "idle", "error": str(e)}).encode())
        else:
            super().do_GET()

    def do_POST(self):
        if self.path == "/api/book_notes":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                BOOK_NOTES_PATH.write_text(body.get("text", ""), encoding="utf-8")
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
            return
        elif self.path == "/api/dismiss-entry":
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

        elif self.path == "/api/toggle-highlight":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            table = body.get("table", "")
            identifier = body.get("identifier", "")
            highlighted = 1 if body.get("highlighted") else 0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not identifier or table not in ("articles", "processed_pdfs"):
                self.wfile.write(json.dumps({"error": "Invalid params"}).encode())
                return

            try:
                conn = get_db()
                if table == "articles":
                    conn.execute("UPDATE articles SET highlighted = ? WHERE id = ?", (highlighted, identifier))
                else:
                    conn.execute("UPDATE processed_pdfs SET highlighted = ? WHERE pdf_filename = ?", (highlighted, identifier))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/toggle-ignore":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            filename = body.get("filename", "")
            ignored = 1 if body.get("ignored") else 0
            add_training = body.get("add_training", False)

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not filename:
                self.wfile.write(json.dumps({"error": "No filename"}).encode())
                return

            try:
                conn = get_db()
                # Check if this was AI-tagged (has confidence score)
                row = conn.execute(
                    "SELECT auto_ignore_confidence FROM processed_pdfs WHERE pdf_filename = ?",
                    (filename,)
                ).fetchone()
                was_ai_tagged = row and row["auto_ignore_confidence"] is not None

                conn.execute(
                    "UPDATE processed_pdfs SET ignored = ? WHERE pdf_filename = ?",
                    (ignored, filename)
                )
                conn.commit()
                conn.close()

                # Update training data if user said yes
                training_updated = False
                if add_training:
                    try:
                        _tf = BASE_DIR / "auto_ignore_training.json"
                        if _tf.exists():
                            td = json.loads(_tf.read_text(encoding="utf-8"))
                        else:
                            td = {"ignore_examples": [], "keep_examples": [], "hard_negatives": [], "max_per_list": 30}
                        _max = td.get("max_per_list", 30)

                        if ignored:
                            # User checked ignore → add to ignore_examples
                            _list = td["ignore_examples"]
                            if filename not in _list:
                                _list.append(filename)
                                if len(_list) > _max:
                                    _list.pop(0)
                        else:
                            # User unchecked ignore
                            if was_ai_tagged:
                                # Correcting AI mistake → hard_negative
                                _list = td["hard_negatives"]
                                if filename not in _list:
                                    _list.append(filename)
                                    if len(_list) > _max:
                                        _list.pop(0)
                            else:
                                # Removing human tag → keep_example
                                _list = td["keep_examples"]
                                if filename not in _list:
                                    _list.append(filename)
                                    if len(_list) > _max:
                                        _list.pop(0)

                        _tf.write_text(json.dumps(td, indent=2), encoding="utf-8")
                        training_updated = True
                    except Exception as te:
                        logging.warning(f"Training update failed: {te}")

                self.wfile.write(json.dumps({"ok": True, "filename": filename, "ignored": ignored, "training_updated": training_updated}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())

        elif self.path == "/api/toggle-photo":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            table = body.get("table", "")
            identifier = body.get("identifier", "")
            has_photo = 1 if body.get("has_photo") else 0

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()

            if not identifier or table not in ("articles", "processed_pdfs"):
                self.wfile.write(json.dumps({"error": "Invalid params"}).encode())
                return

            try:
                conn = get_db()
                if table == "articles":
                    conn.execute("UPDATE articles SET has_photo = ? WHERE id = ?", (has_photo, identifier))
                else:
                    conn.execute("UPDATE processed_pdfs SET has_photo = ? WHERE pdf_filename = ?", (has_photo, identifier))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
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
        elif self.path == "/api/account/save":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                acct_id = body.get("id")
                if acct_id:
                    conn.execute("""
                        UPDATE accounts SET email=?, password=?, label=?, subscription_type=?,
                        active=?, notes=?, updated_at=datetime('now','localtime')
                        WHERE id=?
                    """, (body["email"], body["password"], body.get("label",""),
                          body.get("subscription_type",""), body.get("active",1),
                          body.get("notes",""), acct_id))
                else:
                    conn.execute("""
                        INSERT INTO accounts (email, password, label, subscription_type, active, notes)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (body["email"], body["password"], body.get("label",""),
                          body.get("subscription_type",""), body.get("active",1),
                          body.get("notes","")))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/toggle-active":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute(
                    "UPDATE accounts SET active=?, updated_at=datetime('now','localtime') WHERE id=?",
                    (1 if body.get("active") else 0, body["id"]),
                )
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/delete":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("DELETE FROM accounts WHERE id=?", (body["id"],))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/account/reset-session":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                # Check if daily tracking columns exist before clearing them
                reset_daily = False
                try:
                    col_info = conn.execute("PRAGMA table_info(accounts)").fetchall()
                    col_names = {c["name"] for c in col_info}
                    reset_daily = "urls_today" in col_names
                except Exception:
                    pass
                if reset_daily:
                    conn.execute("""
                        UPDATE accounts SET
                        last_error=NULL, last_error_time=NULL, last_error_screenshot=NULL,
                        urls_today=0, urls_today_date=NULL, last_gathered_time=NULL,
                        updated_at=datetime('now','localtime')
                        WHERE id=?
                    """, (body["id"],))
                else:
                    conn.execute("""
                        UPDATE accounts SET
                        last_error=NULL, last_error_time=NULL, last_error_screenshot=NULL,
                        updated_at=datetime('now','localtime')
                        WHERE id=?
                    """, (body["id"],))
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/settings":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                conn = get_db()
                conn.execute("CREATE TABLE IF NOT EXISTS clipper_state (key TEXT PRIMARY KEY, value TEXT)")
                for key, value in body.items():
                    conn.execute(
                        "INSERT INTO clipper_state (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = ?",
                        (key, str(value), str(value))
                    )
                conn.commit()
                conn.close()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/start":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Check if already running
                if _is_clipper_running():
                    self.wfile.write(json.dumps({"error": "Clipper is already running."}).encode())
                    return
                # Clear stop flag if present
                if STOP_FLAG.exists():
                    if STOP_FLAG.is_dir():
                        STOP_FLAG.rmdir()
                    else:
                        STOP_FLAG.unlink()
                # Build command
                account = body.get("account", "").strip()
                date_start = body.get("date_start", "").strip()
                date_end = body.get("date_end", "").strip()
                cmd = ["python", "clip_and_extract.py", "0"]
                if date_start:
                    cmd.append(date_start)
                    cmd.append(date_end or "2025-12-31")
                    if account:
                        cmd.append(account)
                elif account:
                    cmd.extend(["", "", account])
                # Launch as independent process (CREATE_NEW_PROCESS_GROUP so it survives
                # parent exit, but NOT DETACHED_PROCESS which hides the window and
                # prevents Chrome from getting focus for mouse interactions)
                subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
                msg = f"Clipper started"
                if account:
                    msg += f" with {account}"
                else:
                    msg += " with auto-rotate"
                if date_start:
                    msg += f" ({date_start} to {date_end or '2025-12-31'})"
                self.wfile.write(json.dumps({"ok": True, "message": msg}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/stop":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                STOP_FLAG.mkdir(exist_ok=True)
                self.wfile.write(json.dumps({"ok": True, "message": "Stop flag set. Clipper will stop after current page."}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/set_target":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                try:
                    target = int(body.get("target", 0))
                except Exception:
                    target = 0
                target = max(0, target)
                date_start = (body.get("date_start") or "").strip()
                date_end = (body.get("date_end") or "").strip()
                # Refuse to launch if there are zero pages to clip in the date range
                if target > 0 and date_start and date_end:
                    chk = get_db()
                    try:
                        avail = chk.execute(
                            "SELECT COUNT(*) AS c FROM processed_pdfs "
                            "WHERE (clipped = 0 OR clipped IS NULL) "
                            "AND (ignored IS NULL OR ignored = 0) "
                            "AND url IS NOT NULL AND url != '' "
                            "AND date_str >= ? AND date_str <= ?",
                            (date_start, date_end),
                        ).fetchone()["c"]
                    finally:
                        chk.close()
                    if avail == 0:
                        self.wfile.write(json.dumps({
                            "error": f"No unclipped pages in {date_start} to {date_end}. Check date range."
                        }).encode())
                        return
                _set_state("instances_target", target)
                _set_state("instances_date_start", date_start)
                _set_state("instances_date_end", date_end)
                if target > 0:
                    # Clear global stop flag when setting a positive target,
                    # otherwise autoscale_tick and _spawn_instance will refuse to run.
                    try:
                        if GLOBAL_STOP_FLAG.exists():
                            if GLOBAL_STOP_FLAG.is_dir():
                                GLOBAL_STOP_FLAG.rmdir()
                            else:
                                GLOBAL_STOP_FLAG.unlink()
                    except Exception:
                        pass
                else:
                    # Target=0: set global stop flag to prevent any new spawns
                    # and ensure all instances stop promptly.
                    try:
                        GLOBAL_STOP_FLAG.mkdir(exist_ok=True)
                    except Exception:
                        pass
                # If lowering target, stop the oldest extras immediately.
                _sweep_stale_claims()
                conn = get_db()
                try:
                    rows = conn.execute(
                        "SELECT slot_id, started_at FROM clipper_instances "
                        "ORDER BY COALESCE(started_at, '') DESC"
                    ).fetchall()
                finally:
                    conn.close()
                current = len(rows)
                stopped = []
                if target < current:
                    # stop the oldest ones (sort ascending by started_at)
                    to_stop = sorted(rows, key=lambda r: r["started_at"] or "")[: current - target]
                    for r in to_stop:
                        try:
                            _instance_stop_flag(r["slot_id"]).mkdir(exist_ok=True)
                            stopped.append(r["slot_id"])
                        except Exception:
                            pass
                if target == 0 and current > 0:
                    # Belt-and-suspenders: set per-slot flags for ALL instances
                    for r in rows:
                        try:
                            _instance_stop_flag(r["slot_id"]).mkdir(exist_ok=True)
                            if r["slot_id"] not in stopped:
                                stopped.append(r["slot_id"])
                        except Exception:
                            pass
                # Trigger an immediate autoscale tick to spawn if needed.
                try:
                    _autoscale_tick()
                except Exception:
                    pass
                self.wfile.write(json.dumps({
                    "ok": True,
                    "target": target,
                    "previous": current,
                    "stopped": stopped,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/add":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _sweep_stale_claims()
                date_start = (body.get("date_start") or "").strip()
                date_end = (body.get("date_end") or "").strip()
                max_pages = str(body.get("max_pages") or "0").strip() or "0"

                slot_id = _next_free_slot_id()
                # Wipe the per-slot Chrome profile to guarantee a clean launch.
                # A stale/locked profile from a crashed or force-killed prior
                # run causes undetected_chromedriver to fail with "Browser
                # window not found" or attach to an existing Chrome instance.
                try:
                    import shutil as _shutil
                    _profile_dir = RUNTIME_DIR / f"chrome_temp_profile_clipper_{slot_id}"
                    if _profile_dir.exists():
                        _shutil.rmtree(_profile_dir, ignore_errors=True)
                except Exception:
                    pass
                # Reserve the slot immediately so concurrent /add calls don't
                # race and pick the same slot_id before the child writes its row.
                try:
                    conn = sqlite3.connect(str(DB_PATH))
                    conn.execute(
                        "INSERT OR REPLACE INTO clipper_instances "
                        "(slot_id, pid, status, started_at, heartbeat_at) "
                        "VALUES (?, ?, ?, datetime('now','localtime'), datetime('now','localtime'))",
                        (slot_id, 0, "spawning"),
                    )
                    conn.commit()
                    conn.close()
                except Exception:
                    pass
                # Clear any stop flags that would immediately halt this worker:
                # per-slot, global, and legacy single-instance flags.
                for flag in (_instance_stop_flag(slot_id), GLOBAL_STOP_FLAG, STOP_FLAG):
                    try:
                        if flag.exists():
                            if flag.is_dir():
                                _shutil.rmtree(flag, ignore_errors=True)
                            else:
                                flag.unlink()
                    except Exception:
                        pass

                cmd = [
                    "python", "clip_and_extract.py",
                    f"--slot-id={slot_id}",
                    max_pages,
                ]
                if date_start:
                    cmd.append(date_start)
                    cmd.append(date_end or "2025-12-31")

                # Detach so the worker survives the dashboard closing.
                # Hide the worker's console window (STARTF_USESHOWWINDOW +
                # SW_HIDE) but keep real stdio handles so libraries that write
                # to stdout don't crash. CREATE_NO_WINDOW would kill stdio.
                _si = subprocess.STARTUPINFO()
                _si.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                _si.wShowWindow = 0  # SW_HIDE
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    startupinfo=_si,
                )
                self.wfile.write(json.dumps({
                    "ok": True,
                    "slot_id": slot_id,
                    "pid": proc.pid,
                    "cmd": cmd,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/stop":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                slot_id = str(body.get("slot_id", "")).strip()
                if not slot_id:
                    self.wfile.write(json.dumps({"error": "slot_id required"}).encode())
                    return
                flag = _instance_stop_flag(slot_id)
                flag.mkdir(exist_ok=True)
                self.wfile.write(json.dumps({
                    "ok": True,
                    "message": f"Stop flag set for slot {slot_id}. Instance will stop after current page.",
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/stop_all":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                GLOBAL_STOP_FLAG.mkdir(exist_ok=True)
                # Also zero the autoscale target so the background loop
                # stops wanting to replace stopped instances.
                _set_state("instances_target", "0")
                # Set per-slot flags too — belt and suspenders
                try:
                    conn = sqlite3.connect(str(DB_PATH), timeout=10)
                    slots = [r[0] for r in conn.execute(
                        "SELECT slot_id FROM clipper_instances"
                    ).fetchall()]
                    conn.close()
                    for sid in slots:
                        _instance_stop_flag(sid).mkdir(exist_ok=True)
                except Exception:
                    pass
                self.wfile.write(json.dumps({
                    "ok": True,
                    "message": "Global stop flag set and target=0. All instances will stop.",
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/clear_stop_all":
            # Convenience: remove the global stop flag so new instances can start.
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                if GLOBAL_STOP_FLAG.exists():
                    if GLOBAL_STOP_FLAG.is_dir():
                        GLOBAL_STOP_FLAG.rmdir()
                    else:
                        GLOBAL_STOP_FLAG.unlink()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/instances/reset_stale":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                import shutil as _shutil
                conn = sqlite3.connect(str(DB_PATH), timeout=10)
                conn.row_factory = sqlite3.Row
                # Get ALL clipper instances (kill everything, not just dead PIDs)
                instances = conn.execute("SELECT slot_id, pid, account_email FROM clipper_instances").fetchall()
                killed_pids = []
                slot_ids = []
                for inst in instances:
                    slot_ids.append(inst["slot_id"])
                    pid = int(inst["pid"] or 0)
                    if pid and _pid_alive(pid):
                        try:
                            os.kill(pid, signal.SIGTERM)
                            killed_pids.append(pid)
                        except Exception:
                            pass
                # Also kill any orphan chrome processes from clipper profiles
                try:
                    import subprocess as _sp
                    wmic_out = _sp.check_output(
                        'wmic process where "name=\'chrome.exe\'" get ProcessId,CommandLine /format:csv',
                        shell=True, text=True, timeout=10
                    )
                    for line in wmic_out.splitlines():
                        if "chrome_temp_profile_clipper" in line:
                            parts = line.strip().split(",")
                            try:
                                cpid = int(parts[-1])
                                os.kill(cpid, signal.SIGTERM)
                                killed_pids.append(cpid)
                            except Exception:
                                pass
                except Exception:
                    pass
                # Clear all account claims
                claimed = conn.execute("SELECT email FROM accounts WHERE in_use_by IS NOT NULL").fetchall()
                cleared_claims = len(claimed)
                conn.execute("UPDATE accounts SET in_use_by = NULL, in_use_since = NULL, in_use_pid = NULL")
                # Clear all page claims
                conn.execute("UPDATE processed_pdfs SET claimed_by = NULL, claimed_at = NULL, claimed_pid = NULL WHERE claimed_by IS NOT NULL")
                # Delete all clipper instances
                conn.execute("DELETE FROM clipper_instances")
                # Zero target
                conn.execute("INSERT OR REPLACE INTO clipper_state (key, value) VALUES ('instances_target', '0')")
                conn.commit()
                conn.close()
                # Clear global stop flag
                try:
                    if GLOBAL_STOP_FLAG.exists():
                        if GLOBAL_STOP_FLAG.is_dir():
                            GLOBAL_STOP_FLAG.rmdir()
                        else:
                            GLOBAL_STOP_FLAG.unlink()
                except Exception:
                    pass
                # Clear per-slot stop flags
                for sid in slot_ids:
                    try:
                        sf = _instance_stop_flag(sid)
                        if sf.exists():
                            if sf.is_dir():
                                sf.rmdir()
                            else:
                                sf.unlink()
                    except Exception:
                        pass
                # Delete Chrome temp profiles (cookies, sessions, cache)
                profiles_cleared = 0
                for p in RUNTIME_DIR.glob("chrome_temp_profile_clipper*"):
                    try:
                        _shutil.rmtree(p)
                        profiles_cleared += 1
                    except Exception:
                        pass
                self.wfile.write(json.dumps({
                    "ok": True,
                    "killed_processes": len(killed_pids),
                    "cleared_instances": len(slot_ids),
                    "cleared_claims": cleared_claims,
                    "profiles_cleared": profiles_cleared,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/reextract/start":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Remove stop flag if present
                stop_flag = RUNTIME_DIR / "stop_reextract"
                if stop_flag.exists():
                    stop_flag.unlink(missing_ok=True)
                # Launch as background process
                subprocess.Popen(
                    [sys.executable, str(BASE_DIR / "reextract_no_articles.py")],
                    cwd=str(BASE_DIR),
                    creationflags=_NO_WINDOW,
                )
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/reextract/stop":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                stop_flag = RUNTIME_DIR / "stop_reextract"
                RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
                stop_flag.touch()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/clipper/kill":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Find and kill clip_and_extract.py processes
                result = subprocess.run(
                    ["wmic", "process", "where", "name='python.exe'", "get", "ProcessId,CommandLine"],
                    capture_output=True, text=True, timeout=5,
                    creationflags=_NO_WINDOW,
                )
                killed = []
                for line in result.stdout.splitlines():
                    if "clip_and_extract" in line:
                        parts = line.strip().split()
                        for part in parts:
                            if part.isdigit():
                                pid = int(part)
                                try:
                                    os.kill(pid, signal.SIGTERM)
                                    killed.append(pid)
                                except Exception:
                                    pass
                self.wfile.write(json.dumps({"ok": True, "killed_pids": killed}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/start":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                date_start = (body.get("date_start") or "").strip()
                date_end = (body.get("date_end") or "").strip()
                restart_every = int(body.get("restart_every", 100) or 100)
                max_urls = int(body.get("max_urls", 1500) or 1500)
                if not date_start or not date_end:
                    self.wfile.write(json.dumps(
                        {"error": "Start and end dates are required."}
                    ).encode())
                    return
                # Check if already running
                try:
                    conn = get_db()
                    status_row = conn.execute(
                        "SELECT value FROM clipper_state WHERE key = 'collector_status'"
                    ).fetchone()
                    pid_row = conn.execute(
                        "SELECT value FROM clipper_state WHERE key = 'collector_pid'"
                    ).fetchone()
                    conn.close()
                    old_status = status_row["value"] if status_row else ""
                    old_pid = int(pid_row["value"]) if pid_row and pid_row["value"] else 0
                    if old_status == "running" and old_pid and _pid_alive(old_pid):
                        self.wfile.write(json.dumps(
                            {"error": f"Collector already running (PID {old_pid})."}
                        ).encode())
                        return
                except Exception:
                    pass
                # Clear stop flag
                _coll_stop = RUNTIME_DIR / "stop_collector"
                try:
                    if _coll_stop.exists():
                        if _coll_stop.is_dir():
                            _coll_stop.rmdir()
                        else:
                            _coll_stop.unlink()
                except Exception:
                    pass
                # Claim an account — use specified email or auto-select
                chosen_email = (body.get("email") or "").strip()
                conn = get_db()
                try:
                    if chosen_email:
                        acct = conn.execute(
                            "SELECT email, password FROM accounts "
                            "WHERE email = ? AND active = 1",
                            (chosen_email,),
                        ).fetchone()
                    else:
                        acct = conn.execute(
                            "SELECT email, password FROM accounts "
                            "WHERE active = 1 AND in_use_by IS NULL "
                            "ORDER BY total_clips ASC LIMIT 1"
                        ).fetchone()
                finally:
                    conn.close()
                if not acct:
                    self.wfile.write(json.dumps(
                        {"error": "No available account." + (" Check if it's active." if chosen_email else " All are in use or inactive.")}
                    ).encode())
                    return
                email = acct["email"]
                # Spawn new collector (uses browser_session for login/rotation)
                cmd = [
                    "python", "collect_urls.py",
                    "--start", date_start,
                    "--end", date_end,
                    "--account", email,
                    "--restart-every", str(restart_every),
                    "--max-urls", str(max_urls),
                ]
                _si_coll = subprocess.STARTUPINFO()
                _si_coll.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                _si_coll.wShowWindow = 0  # SW_HIDE
                proc = subprocess.Popen(
                    cmd,
                    cwd=str(BASE_DIR),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    startupinfo=_si_coll,
                )
                import time as _time
                _time.sleep(1)
                if proc.poll() is not None:
                    self.wfile.write(json.dumps(
                        {"error": "Collector process died immediately."}
                    ).encode())
                    return
                self.wfile.write(json.dumps({
                    "ok": True,
                    "pid": proc.pid,
                    "account": email,
                    "date_start": date_start,
                    "date_end": date_end,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/stop":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _coll_stop = RUNTIME_DIR / "stop_collector"
                _coll_stop.mkdir(exist_ok=True)
                self.wfile.write(json.dumps({
                    "ok": True,
                    "message": "Stop flag set. Collector will stop after current batch.",
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances/set_target":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                try:
                    target = int(body.get("target", 0))
                except Exception:
                    target = 0
                target = max(0, target)
                date_start = (body.get("date_start") or "").strip()
                date_end = (body.get("date_end") or "").strip()
                max_urls = int(body.get("max_urls", 1500) or 1500)
                restart_every = int(body.get("restart_every", 100) or 100)
                _set_state("collector_instances_target", target)
                _set_state("collector_instances_date_start", date_start)
                _set_state("collector_instances_date_end", date_end)
                _set_state("collector_instances_max_urls", max_urls)
                _set_state("collector_instances_restart_every", restart_every)
                if target > 0:
                    try:
                        if COLLECTOR_GLOBAL_STOP_FLAG.exists():
                            if COLLECTOR_GLOBAL_STOP_FLAG.is_dir():
                                import shutil as _sh
                                _sh.rmtree(COLLECTOR_GLOBAL_STOP_FLAG, ignore_errors=True)
                            else:
                                COLLECTOR_GLOBAL_STOP_FLAG.unlink()
                    except Exception:
                        pass
                    try:
                        _legacy = RUNTIME_DIR / "stop_collector"
                        if _legacy.exists():
                            import shutil as _sh2
                            if _legacy.is_dir():
                                _sh2.rmtree(_legacy, ignore_errors=True)
                            else:
                                _legacy.unlink()
                    except Exception:
                        pass
                else:
                    # Target=0: set global stop flag and per-slot flags
                    try:
                        COLLECTOR_GLOBAL_STOP_FLAG.mkdir(exist_ok=True)
                    except Exception:
                        pass
                    try:
                        _cconn = get_db()
                        _cslots = [r[0] for r in _cconn.execute(
                            "SELECT slot_id FROM collector_instances"
                        ).fetchall()]
                        _cconn.close()
                        for _csid in _cslots:
                            try:
                                _collector_instance_stop_flag(_csid).touch()
                            except Exception:
                                pass
                    except Exception:
                        pass
                self.wfile.write(json.dumps({
                    "ok": True, "target": target,
                    "date_start": date_start, "date_end": date_end,
                }).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances/stop":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                slot = body.get("slot_id", "")
                if slot:
                    flag = _collector_instance_stop_flag(slot)
                    RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
                    flag.touch()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances/stop_all":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _set_state("collector_instances_target", "0")
                RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
                COLLECTOR_GLOBAL_STOP_FLAG.touch()
                (RUNTIME_DIR / "stop_collector").mkdir(exist_ok=True)
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances/clear_stop_all":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                if COLLECTOR_GLOBAL_STOP_FLAG.exists():
                    COLLECTOR_GLOBAL_STOP_FLAG.unlink()
                _legacy = RUNTIME_DIR / "stop_collector"
                if _legacy.exists():
                    import shutil as _shutil2
                    if _legacy.is_dir():
                        _shutil2.rmtree(_legacy, ignore_errors=True)
                    else:
                        _legacy.unlink()
                self.wfile.write(json.dumps({"ok": True}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/collector/instances/reset_stale":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _set_state("collector_instances_target", "0")
                RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
                COLLECTOR_GLOBAL_STOP_FLAG.touch()
                conn = get_db()
                try:
                    rows = conn.execute("SELECT slot_id, pid FROM collector_instances").fetchall()
                    killed = []
                    for r in rows:
                        pid = int(r["pid"] or 0)
                        if pid and _pid_alive(pid):
                            try:
                                os.kill(pid, signal.SIGTERM)
                                killed.append(pid)
                            except Exception:
                                pass
                    conn.execute("DELETE FROM collector_instances")
                    conn.commit()
                finally:
                    conn.close()
                self.wfile.write(json.dumps({"ok": True, "killed": killed}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/auto-ignore/start":
            content_len = int(self.headers.get("Content-Length", 0))
            body = json.loads(self.rfile.read(content_len)) if content_len else {}
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                # Clear any leftover stop flag
                _ai_stop = RUNTIME_DIR / "stop_auto_ignore"
                if _ai_stop.exists():
                    _shutil.rmtree(_ai_stop, ignore_errors=True)

                date_start = body.get("date_start", "1921-01-01")
                date_end = body.get("date_end", "1921-12-31")
                batch = body.get("batch", 20)
                dry = body.get("dry", False)

                cmd = [sys.executable, str(BASE_DIR / "auto_ignore.py"),
                       "--date-start", str(date_start), "--date-end", str(date_end),
                       "--batch", str(batch)]
                if dry:
                    cmd += ["--dry"]

                _si_ai = subprocess.STARTUPINFO()
                _si_ai.dwFlags |= subprocess.STARTF_USESHOWWINDOW
                _si_ai.wShowWindow = 0
                proc = subprocess.Popen(
                    cmd, cwd=str(BASE_DIR),
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                    startupinfo=_si_ai,
                )
                self.wfile.write(json.dumps({"ok": True, "pid": proc.pid}).encode())
            except Exception as e:
                self.wfile.write(json.dumps({"error": str(e)}).encode())
        elif self.path == "/api/auto-ignore/stop":
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.send_header("Access-Control-Allow-Origin", "*")
            self.end_headers()
            try:
                _ai_stop = RUNTIME_DIR / "stop_auto_ignore"
                _ai_stop.mkdir(exist_ok=True)
                self.wfile.write(json.dumps({
                    "ok": True,
                    "message": "Stop flag set. Auto-ignore will stop after current batch.",
                }).encode())
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
    # Start the autoscale background thread. It reads instances_target
    # from clipper_state every 30s and spawns missing instances.
    import threading as _threading
    _t = _threading.Thread(target=_autoscale_loop, daemon=True)
    _t.start()
    print("Autoscale thread started (30s tick).")
    _ct = _threading.Thread(target=_collector_autoscale_loop, daemon=True)
    _ct.start()
    print("Collector autoscale thread started (30s tick).")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopped.")
        server.server_close()


if __name__ == "__main__":
    main()
