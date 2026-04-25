"""
migrate_phase1.py — Creates Librarian enrichment tables and seeds era data.
Safe to run multiple times: uses IF NOT EXISTS and checks COUNT before inserting.
"""

import sqlite3
from pathlib import Path

DB_PATH = Path(r"C:\lake_worth\lake_worth.db")


def run():
    conn = sqlite3.connect(str(DB_PATH))
    conn.execute("PRAGMA journal_mode=WAL")
    cur = conn.cursor()

    created = []
    skipped = []

    tables = {
        "enrichment_runs": """
            CREATE TABLE IF NOT EXISTS enrichment_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                agent_name TEXT,
                prompt_version TEXT,
                prompt_hash TEXT,
                model TEXT,
                started_at TEXT,
                completed_at TEXT,
                articles_processed INTEGER DEFAULT 0,
                notes TEXT
            )
        """,
        "article_type": """
            CREATE TABLE IF NOT EXISTS article_type (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER REFERENCES articles(id),
                type TEXT,
                confidence FLOAT,
                enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
            )
        """,
        "article_flags": """
            CREATE TABLE IF NOT EXISTS article_flags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER REFERENCES articles(id),
                importance INTEGER,
                interesting INTEGER DEFAULT 0,
                ironic INTEGER DEFAULT 0,
                tragic INTEGER DEFAULT 0,
                comic INTEGER DEFAULT 0,
                cautionary INTEGER DEFAULT 0,
                scandalous INTEGER DEFAULT 0,
                notable_quote INTEGER DEFAULT 0,
                enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
            )
        """,
        "eras": """
            CREATE TABLE IF NOT EXISTS eras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                slug TEXT,
                date_start TEXT,
                date_end TEXT,
                description TEXT
            )
        """,
        "article_eras": """
            CREATE TABLE IF NOT EXISTS article_eras (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER REFERENCES articles(id),
                era_id INTEGER REFERENCES eras(id),
                confidence FLOAT,
                enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
            )
        """,
        "entities": """
            CREATE TABLE IF NOT EXISTS entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                type TEXT,
                date_first TEXT,
                date_last TEXT,
                description TEXT,
                notes TEXT
            )
        """,
        "article_entities": """
            CREATE TABLE IF NOT EXISTS article_entities (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                article_id INTEGER REFERENCES articles(id),
                entity_id INTEGER REFERENCES entities(id),
                mention_text TEXT,
                source TEXT DEFAULT 'ai',
                verified INTEGER DEFAULT 0,
                enrichment_run_id INTEGER REFERENCES enrichment_runs(id)
            )
        """,
    }

    # Check which tables already exist
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    existing = {r[0] for r in cur.fetchall()}

    for table_name, ddl in tables.items():
        cur.execute(ddl)
        if table_name in existing:
            skipped.append(table_name)
        else:
            created.append(table_name)

    conn.commit()

    # Seed eras — only if table is empty
    era_count = cur.execute("SELECT COUNT(*) FROM eras").fetchone()[0]
    eras_inserted = 0
    if era_count == 0:
        eras = [
            (
                "Impoundment",
                "impoundment",
                "1909-01-01",
                "1914-12-31",
                "The fire, the fight. Reservoir vs. artesian wells. Property battles, dam construction, the lake fills. Defined by conflict and necessity.",
            ),
            (
                "Discovery",
                "discovery",
                "1914-01-01",
                "1917-12-31",
                "Lake is new, no plan for it. Organic recreation emerges — fishing, camping, swimming. First camps appear. Flooding lawsuits. City figures out what it has.",
            ),
            (
                "Carnival",
                "carnival",
                "1917-01-01",
                "1929-12-31",
                "Amusement park opens. Interurban brings day-trippers. Camps multiply. Prohibition and Jacksboro Highway drive bootlegging alongside dancing and boats. Shriners, VIPs, mashing, drama. Peak attendance, peak chaos.",
            ),
            (
                "Depression & War",
                "depression_war",
                "1929-01-01",
                "1945-12-31",
                "Carnival fades. Depression cuts attendance. WWII shifts energy. Lake persists but commercial peak passes.",
            ),
            (
                "Postwar Suburbs",
                "postwar_suburbs",
                "1945-01-01",
                "1970-12-31",
                "Fort Worth grows outward. Lake becomes a neighborhood lake. Siltation accumulates. Operators close.",
            ),
            (
                "Decline",
                "decline",
                "1970-01-01",
                "2000-12-31",
                "Governance failures, inadequate dredging, encroachment. Community treasure quietly deteriorates.",
            ),
            (
                "Renewal",
                "renewal",
                "2000-01-01",
                "2099-12-31",
                "Dredging projects, community advocacy, growing awareness of historical and ecological value.",
            ),
        ]
        cur.executemany(
            "INSERT INTO eras (name, slug, date_start, date_end, description) VALUES (?,?,?,?,?)",
            eras,
        )
        conn.commit()
        eras_inserted = len(eras)

    conn.close()

    print("=== migrate_phase1.py complete ===")
    if created:
        print(f"  Tables created:  {', '.join(created)}")
    if skipped:
        print(f"  Tables skipped (already exist): {', '.join(skipped)}")
    if eras_inserted:
        print(f"  Eras inserted:   {eras_inserted}")
    else:
        print(f"  Eras skipped:    table already had {era_count} rows")


if __name__ == "__main__":
    run()
