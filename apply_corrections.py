"""
Apply manual corrections from corrections.sql to the Lake Worth database.

Usage:
  python apply_corrections.py            — apply all corrections
  python apply_corrections.py --dry-run  — show statements without executing
"""
import argparse
import sqlite3
from pathlib import Path

DB_PATH = Path(r"C:\lake_worth\lake_worth.db")
CORRECTIONS_PATH = Path(r"C:\lake_worth\corrections.sql")


def parse_statements(sql: str) -> list:
    """Parse SQL file into individual statements, skipping comments and blanks."""
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


def main():
    parser = argparse.ArgumentParser(description="Apply corrections.sql to the Lake Worth database")
    parser.add_argument("--dry-run", action="store_true", help="Show statements without executing")
    args = parser.parse_args()

    if not CORRECTIONS_PATH.exists():
        print(f"No corrections file found at {CORRECTIONS_PATH}")
        return

    sql = CORRECTIONS_PATH.read_text(encoding="utf-8")
    statements = parse_statements(sql)

    if not statements:
        print("No corrections to apply.")
        return

    print(f"Found {len(statements)} correction(s).")

    if args.dry_run:
        print("\n[DRY RUN] Would execute:")
        for i, stmt in enumerate(statements, 1):
            print(f"  [{i}] {stmt[:120]}")
        return

    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    applied = 0
    for i, stmt in enumerate(statements, 1):
        try:
            cur = conn.execute(stmt)
            conn.commit()
            print(f"  [{i}] OK ({cur.rowcount} row(s) affected): {stmt[:80].strip()}")
            applied += 1
        except Exception as e:
            print(f"  [{i}] ERROR: {e}")
    conn.close()
    print(f"\nDone: {applied}/{len(statements)} applied.")


if __name__ == "__main__":
    main()
